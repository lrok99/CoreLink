using System.IO.Ports;
using System.Threading.Channels;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using CoreLink.Core.Transport.Events;

namespace CoreLink.Core.Transport
{
    /// <summary>
    /// 串口传输实现。
    /// 设计要点：
    /// - 状态机：Closed -> Opening -> Open -> Closing -> Closed；异常进入 Faulted，致命错误会自动回收资源回到 Closed。
    /// - 收数据：SerialPort.DataReceived(可能并发触发) -> Channel -> 单一消费者任务 -> DataReceived 事件。
    /// - 生命周期：OpenAsync/CloseAsync 通过 _gate 串行化；DataReceived 订阅在 Open 时挂、Close 时摘，保证重开有效。
    /// - Channel 满时采用 DropWrite 并在首次丢失时上报非致命错误（不静默丢数据）。
    /// </summary>
    public class SerialTransport : ITransport
    {
        /// <summary>单次 DataReceived 事件最多读取的轮数（防御性上限，防高速虚拟串口长期占用线程池线程）。</summary>
        private const int MaxReadBurstPerEvent = 4096;
        private readonly SerialPort _serialPort;// 构造后不再为 null，重开复用同一实例
        private readonly string _name;                         // 缓存 PortName，释放后访问 Name 仍安全
        private readonly object _sync = new();
        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1,1);// 串行化 SendAsync
        private readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);// 串行化 Open/Close 状态迁移
        private Channel<byte[]>? _channel;
        private CancellationTokenSource? _cts;
        private Task? _consumerTask;
        private TransportState _state = TransportState.Closed;
        private volatile bool _disposed;
        private int _dropReported;                             // 0/1，丢包上报节流标记
        public string Name => _name;

        public bool IsOpen { get { lock (_sync) { return _state == TransportState.Open; } } }

        public TransportState State { 
            get
            {
                lock (_sync)
                {
                    return _state;
                }
            }
        }

        public event EventHandler<CoreLink.Core.Transport.Events.DataReceivedEventArgs>? DataReceived;
        public event EventHandler<CoreLink.Core.Transport.Events.ErrorEventArgs>? ErrorOccurred;
        public event EventHandler<CoreLink.Core.Transport.Events.TransportStateChangedEventArgs>? StateChanged;

        public SerialTransport(string portName,int baudRate,Parity parity,int dataBits, StopBits stopBits)
        {
            _serialPort = new SerialPort(portName, baudRate, parity, dataBits, stopBits);
            _name = portName;
            // 注意：不在这里订阅 DataReceived。
            // 订阅放在 OpenAsync、退订放在 CloseAsync / Dispose，否则 Close -> Open 后接收路径会永久失效。
        }


        #region IDisposable
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            // 托管资源只在 disposing == true 路径释放（标准 Dispose 模式）
            if (!disposing)
                return;

            try
            {
                try { this._serialPort.DataReceived -= OnPortDataReceived; } catch { }
                try { _cts?.Cancel(); } catch { }
                try { _channel?.Writer.TryComplete(); } catch { }
                try { if (_serialPort?.IsOpen == true) _serialPort.Close(); } catch { }
                //处理消费任务
                var consumer = _consumerTask;
                if (consumer != null && consumer.Id != Task.CurrentId)
                {
                    try
                    {
                        // Task.Wait(timeout) 超时是返回 false，不会抛 TimeoutException
                        if (!consumer.Wait(TimeSpan.FromMilliseconds(500)))
                            Trace.WriteLine("Consumer did not exit within 500ms.");
                    }
                    catch (AggregateException ae)
                    {
                        Trace.WriteLine($"Consumer faulted during dispose: {ae.Flatten().InnerException}");
                    }
                }
                try { _cts?.Dispose(); } catch { }

                TransportState oldState;
                lock (_sync)
                {
                    oldState = _state;
                    _state = TransportState.Closed;
                }
                RaiseStateChanged(oldState, TransportState.Closed);
                try { _serialPort?.Dispose(); } catch { }
            }
            catch(Exception e)
            {
                // Dispose 中不再向上抛错误事件（此时对象已不可信），只留痕迹
                Trace.WriteLine($"Dispose error: {e}");
            }
            // 信号量释放与托管清理同路径（不再用 finally，避免与 !disposing 早退混淆）
            try { _sendLock.Dispose(); } catch { }
            try { _gate.Dispose(); } catch { }
        }

        public async ValueTask DisposeAsync()
        {
            // 铁律：在任何异步清理开始之前置位，阻止并发的 Open/Close/Send 通过 ThrowIf 检查
            _disposed = true;
            await DisposeAsyncCore().ConfigureAwait(false);

            Dispose(false);// _disposed 已置位，直接返回，不会重复清理

            GC.SuppressFinalize(this);
        }

        protected virtual async ValueTask DisposeAsyncCore()
        {
            // 走 CloseAsyncCore（无 disposed 检查），公开入口 CloseAsync 此时已不可用
            try
            {
                await CloseAsyncCore(CancellationToken.None).ConfigureAwait(false);

            }
            catch (Exception e)
            {
                Trace.WriteLine($"DisposeAsyncCore error: {e}");
            }
            // 异步路径同样要释放串口（只走 Dispose(true) 释放的话，DisposeAsync 会漏）
            try { _serialPort.DataReceived -= OnPortDataReceived; } catch { }
            try { _serialPort.Dispose(); } catch { }

            lock (_sync) { _state = TransportState.Closed; }
            try { _sendLock.Dispose(); } catch { }
            try { _gate.Dispose(); } catch { }
        }
        #endregion
        #region Open/Close
        public async Task OpenAsync(CancellationToken cancellationToken=default)
        {
            ObjectDisposedException.ThrowIf(_disposed,this);
            cancellationToken.ThrowIfCancellationRequested();
            await this._gate.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                // 二次检查：等待 _gate 期间对象可能已被 Dispose
                ObjectDisposedException.ThrowIf(_disposed, this);
                // ============ 阶段 1：状态检查与宣告 Opening ============
                TransportState oldState;
                lock (_sync)
                {
                    if (this._state == TransportState.Open || this._state == TransportState.Opening)
                    {
                        return;// 幂等返回
                    }
                    if(this._state == TransportState.Closing)
                    {
                        throw new InvalidOperationException("Transport is in Closing state,wait for close complete");
                    }
                    if (this._state == TransportState.Faulted)
                    {
                        throw new InvalidOperationException("Transport is in Faulted state,call close first");
                    }
                    oldState = this._state;
                    this._state = TransportState.Opening;
                }
                RaiseStateChanged(oldState, TransportState.Opening);
                // ============ 阶段 2：创建资源（锁外，因为可能较慢） ============
                Channel<byte[]>? channel = null;
                CancellationTokenSource? cts = null;
                Task? consumerTask = null;
                bool subscribed = false;
                try
                {
                    channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(1024)
                    {
                        FullMode = BoundedChannelFullMode.DropWrite,// DropWrite 下 TryWrite 返回 false，丢包可观测
                        SingleReader = true,
                        SingleWriter = false// SerialPort.DataReceived 可能在线程池多线程并发触发，写入方不唯一
                    });
                    cts = new CancellationTokenSource();
                    var token = cts.Token;
                    var reader = channel.Reader;
                    consumerTask = Task.Run(() => ProcessChannelAsync(reader, token), CancellationToken.None);
                    // 每次打开都重新订阅（CloseAsync 里会退订）
                    this._serialPort.DataReceived += OnPortDataReceived;
                    subscribed = true;
                    // 真正打开串口（异步化，避免阻塞 UI）
                    await Task.Run(_serialPort.Open, cancellationToken).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();
                    // ============ 阶段 3：提交状态（锁内赋所有共享字段） ============
                    lock (_sync)
                    {
                        this._channel = channel;
                        this._cts = cts;
                        this._consumerTask = consumerTask;
                        this._state = TransportState.Open;
                    }
                    RaiseStateChanged(TransportState.Opening, TransportState.Open);

                }
                catch(OperationCanceledException) when(cancellationToken.IsCancellationRequested)
                {
                    // 用户主动取消：回滚资源，状态恢复 Closed，不报错误事件
                    await RollbackOpenAsync(channel, cts, consumerTask, subscribed).ConfigureAwait(false);
                    lock (_sync) { _state = TransportState.Closed; }
                    RaiseStateChanged(TransportState.Opening, TransportState.Closed, "Open canceled.");
                    throw;
                }
                catch (Exception ex)
                {
                    // 打开失败：回滚资源，进入 Faulted（随后 RaiseError 会触发自动关闭回到 Closed）
                    await RollbackOpenAsync(channel, cts, consumerTask, subscribed).ConfigureAwait(false);
                    lock (_sync) { _state = TransportState.Faulted; }
                    RaiseStateChanged(TransportState.Opening, TransportState.Faulted, ex.Message);
                    RaiseError(ex, isFatal: true);
                    throw;
                }
            }
            finally
            {
                try { _gate.Release(); } 
                catch(ObjectDisposedException) { /* 与 Dispose 交错时信号量可能已释放 */}
            }
            
        }
        private async Task RollbackOpenAsync(Channel<byte[]>? channel, CancellationTokenSource? cts, Task? consumerTask, bool subscribed)
        {
            if(subscribed)
            {
                try { this._serialPort.DataReceived -= OnPortDataReceived; } catch { }
            }
            try { if (this._serialPort?.IsOpen == true) this._serialPort.Close(); } catch { }
            try { cts?.Cancel(); } catch { }
            try { channel?.Writer.Complete(); } catch { }
            try { if (consumerTask is not null) await consumerTask.WaitAsync(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false); } catch { }
            try { cts?.Dispose(); } catch { }
        }
        /// <summary>
        /// 公开入口：先检查 disposed，再进入核心关闭流程。
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return CloseAsyncCore(cancellationToken);

        }
        /// <summary>
        /// 核心关闭流程，不做 disposed 检查——DisposeAsyncCore 在 _disposed 置位后也需要走这条路。
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task CloseAsyncCore(CancellationToken cancellationToken)
        {
            await this._gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // ============ 1. 状态检查 + 宣告 Closing ============
                TransportState oldState;
                lock (_sync)
                {
                    if (_state is not (TransportState.Open or TransportState.Opening or TransportState.Faulted))
                        return;   // Closed / Closing 幂等返回

                    oldState = _state;
                    _state = TransportState.Closing;
                }
                RaiseStateChanged(oldState, TransportState.Closing);
                // ============ 2. 快照并摘牌（锁内取走并置 null，锁外使用） ============
                Channel<byte[]>? channel;
                CancellationTokenSource? cts;
                Task? consumerTask;
                lock (_sync)
                {
                    channel = _channel;
                    cts = _cts;
                    consumerTask = _consumerTask;
                    _channel = null;      // 先摘牌：此后 OnPortDataReceived 立即静默返回
                    _cts = null;
                    _consumerTask = null;
                }
                // ============ 3. 退订 + 关串口（阻止新数据源，最先执行） ============
                try { _serialPort.DataReceived -= OnPortDataReceived; } catch { }
                try { if (_serialPort.IsOpen) _serialPort.Close(); }
                catch (Exception ex) { Trace.WriteLine($"Close port error: {ex}"); }
                // ============ 4. Complete + Cancel ============
                try { channel?.Writer.TryComplete(); } catch { }
                try { cts?.Cancel(); } catch { }
                // ============ 5. 等待消费者退出（从消费者任务内调用 Close 时跳过等待，避免死锁） ============
                if (consumerTask is not null && consumerTask.Id != Task.CurrentId)
                {
                    try
                    {
                        await consumerTask.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false);
                    }
                    catch (TimeoutException)
                    {
                        Trace.WriteLine("Consumer did not exit within 500ms.");
                    }
                    catch (OperationCanceledException)
                    {
                        // 用户取消等待，放弃；后台消费者自行收尾
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine($"Consumer wait error: {ex}");
                    }
                }
                try { cts?.Dispose(); } catch { }

                // ============ 6. 状态 -> Closed ============
                lock (_sync) { _state = TransportState.Closed; }
                RaiseStateChanged(TransportState.Closing, TransportState.Closed);
            }
            finally
            {
                try { _gate.Release(); }
                catch (ObjectDisposedException) { /* 与 Dispose 交错时信号量可能已释放 */ }
            }
        }
        #endregion

        #region SendAsync
        public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();
            if(data.Length == 0)
            {
                return;
            }

            lock (_sync)
            {
                if (_state != TransportState.Open)
                    throw new InvalidOperationException($"Cannot send in state {_state}.");
            }
            await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await this._serialPort.BaseStream.WriteAsync(data, cancellationToken).ConfigureAwait(false);
            }
            catch (IOException ex)
            {
                RaiseError(ex, isFatal: true);   // 拔线/驱动错误 = 致命
                throw;
            }
            catch (OperationCanceledException)
            {
                throw;   // 取消是正常流程，不报错
            }
            catch (Exception ex)
            {
                RaiseError(ex, isFatal: false);
                throw;
            }
            finally
            {
                try { _sendLock.Release(); } catch (ObjectDisposedException) { /* 与 Dispose 交错时信号量可能已释放 */ }
            }
        }
        #endregion

        #region SerialPort.DataReceived -> Channel -> DataReceived
        private void OnPortDataReceived(object? sender, SerialDataReceivedEventArgs e)
        {
            ChannelWriter<byte[]>? writer;
            CancellationToken cancellationToken;
            lock (_sync)
            {

                writer = _channel?.Writer;
                cancellationToken = _cts?.Token ?? default;
            }

            if ( writer is null || cancellationToken.IsCancellationRequested || !this._serialPort.IsOpen)
                return;

            try
            {
                int loopGuard = 0;
                while (this._serialPort.IsOpen && this._serialPort.BytesToRead > 0 && loopGuard++ < MaxReadBurstPerEvent)
                {
                    int bytesToRead = this._serialPort.BytesToRead;
                    var data = new byte[bytesToRead];
                    int bytesRead = this._serialPort.Read(data, 0, bytesToRead);
                    if(bytesRead <= 0)
                        break;   // 直接退出，finally 会归还，绝不手动 Return)
                    if(bytesRead < bytesToRead)
                    {
                        Array.Resize(ref data, bytesRead);
                    }
                    if(writer.TryWrite(data))
                    {
                        Interlocked.Exchange(ref _dropReported, 0); // 成功写入，重置丢包上报节流标记
                    }
                    else
                    {
                        // 缓冲满被 DropWrite 丢弃：只在连续丢失的第一次上报，避免事件风暴
                        if (Interlocked.Exchange(ref _dropReported, 1) == 0)
                        {
                            RaiseError(new InvalidOperationException(
                                "Receive buffer full; incoming serial data is being dropped."), isFatal: false);
                        }
                    }
                }
            }
            catch (InvalidOperationException)
            {
                // 端口在读取过程中被关闭，正常退出
            }
            catch (Exception ex)
            {
                // 关闭竞态（Closing/Closed）下的读异常不属于故障，静默忽略；
                // 其余（拔线、驱动崩溃等）上报并触发自动关闭
                var state = State;
                if (state is not (TransportState.Closing or TransportState.Closed))
                    RaiseError(ex, isFatal: true);
            }
        }

        private async Task ProcessChannelAsync(ChannelReader<byte[]> reader, CancellationToken ct) {
            try
            {
                await foreach (var data in reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    var handlers = DataReceived?.GetInvocationList();
                    if (handlers is null) continue;
                    // 注意：订阅者在本任务上同步逐个调用，且共享同一个 byte[]。
                    // 约定：订阅者必须快速返回、不得修改 Data、不得保留引用（需要时自行拷贝）。
                    foreach (var handler in handlers)
                    {
                        try
                        {
                            ((EventHandler<CoreLink.Core.Transport.Events.DataReceivedEventArgs>)handler).Invoke(this, new CoreLink.Core.Transport.Events.DataReceivedEventArgs { Data = data, SourceName = Name, Timestamp = DateTime.UtcNow });
                        }
                        catch (Exception ex)
                        {
                            Trace.WriteLine($"Subscriber error: {ex}");
                        }
                    }
                }
            }
            catch (OperationCanceledException) { /* 正常取消 */ }
            catch (Exception ex)
            {
                // 消费者崩溃 -> 上报 fatal -> 触发自动关闭。
                // 自动关闭在独立线程池任务上执行，等待本任务退出不会死锁（本任务已在 catch 中，即将结束）。
                RaiseError(ex, isFatal: true);
            }
            
        }
        #endregion

        #region Error/StateChanged
        private void RaiseError(Exception ex,bool isFatal)
        {
            TransportState oldState;
            lock (_sync)
            {
                oldState = _state;
                if (isFatal && _state is TransportState.Open or TransportState.Opening)
                {
                    _state = TransportState.Faulted;
                }
            }
            try
            {
                ErrorOccurred?.Invoke(this, new Events.ErrorEventArgs { Exception = ex, SourceName = Name, IsFatal = isFatal });
            }catch(Exception e)
            {
                Trace.WriteLine($"ErrorOccurred subscriber error: {e}");
            }
            
            if (isFatal && oldState != TransportState.Faulted)
            {
                RaiseStateChanged(oldState, TransportState.Faulted, ex.Message);
            }
            // 致命错误后自动回收资源（Faulted -> Closing -> Closed），不依赖调用方手动 CloseAsync。
            // 自动关闭与用户手动 Open/Close 都经过 _gate 串行化，不会交错。
            // 注：Faulted 状态存在时间可能很短，诊断信息以 ErrorOccurred 事件为准
            if(isFatal && !_disposed)
            {
                Task.Run(async () =>
                {
                    try
                    {
                        await CloseAsyncCore(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        Trace.WriteLine($"Auto close after fatal error failed: {e}");
                    }
                });
            }
        }


        private void RaiseStateChanged(TransportState oldState, TransportState newState, string? reason = null)
        {
            if(oldState == newState)
            {
                return;
            }
            try
            {
                StateChanged?.Invoke(this, new TransportStateChangedEventArgs { OldState = oldState, NewState = newState, Reason = reason??string.Empty, SourceName = Name });
            }catch(Exception ex)
            {
                Trace.WriteLine($"StateChanged subscriber error: {ex}");
            }
        }
        #endregion
    }
}
