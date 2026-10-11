using System.Buffers;

using System.Diagnostics;
using System.IO.Ports;
using System.Text;
using System.Threading;
using System.Threading.Channels;

namespace CoreLink.Core;

public class SerialPortEngine : IDisposable
{
    /// <summary>
    /// 原始字节流事件。注意：此事件触发并不代表一个完整的数据包。
    /// 粘包/拆包（如 Modbus、自定义帧头）请在上层协议解析器中处理，引擎只负责透传。
    /// </summary>
    public sealed class DataReceivedEventArgs : EventArgs
    {
        public required byte[] Data { get; init; }
        public required string PortName { get; init; }
    }

    private sealed class ReceivedItem
    {
        public required byte[] Data { get; init; }
        public required string PortName { get; init; }
    }

    private const int DefaultQueueCapacity = 1024;
    private readonly object _sync = new();
    private readonly SemaphoreSlim _sendSemaphore = new(1, 1);
    private SerialPort? _serialPort;
    private CancellationTokenSource? _cts;
    private Channel<ReceivedItem>? _channel;
    private Task? _consumerTask;
    private long _droppedItems;
    private bool _disposed;

    public event EventHandler<DataReceivedEventArgs>? DataReceived;

    public void Open(string portName, int baudRate, Parity parity, int dataBits, StopBits stopBits)
    {
        ArgumentException.ThrowIfNullOrEmpty(portName);
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_serialPort?.IsOpen == true)
            {
                throw new InvalidOperationException("Serial port is already open.");
            }

            if (_consumerTask is { IsCompleted: false })
            {
                throw new InvalidOperationException("The previous consumer task has not completed. Call Close first.");
            }

            _serialPort?.Dispose();
            _serialPort = null;
            _cts?.Dispose();
            _cts = null;
            _consumerTask = null;
            _channel = null;


            var port = new SerialPort(portName, baudRate, parity, dataBits, stopBits);
            var cts = new CancellationTokenSource();

            try
            {
                port.DataReceived += OnDataReceived;
                port.Open();

                _serialPort = port;
                _channel = Channel.CreateBounded<ReceivedItem>(new BoundedChannelOptions(DefaultQueueCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true,
                    AllowSynchronousContinuations = false
                });
                _cts = cts;
                _consumerTask = ProcessQueue(_channel.Reader, cts.Token);
            }
            catch
            {
                port.DataReceived -= OnDataReceived;
                port.Dispose();
                _serialPort = null;
                _channel = null;
                _cts = null;
                cts.Dispose();
                throw;
            }
        }
    }

    protected virtual void OnDataReceived(DataReceivedEventArgs e)
    {
        DataReceived?.Invoke(this, e);
    }

    private void OnDataReceived(object? sender, SerialDataReceivedEventArgs e)
    {
        lock (_sync)
        {
            var port = _serialPort;
            var cancellationToken = _cts?.Token ?? default;
            var channel = _channel;

            if (port is null || channel is null || !port.IsOpen || cancellationToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                while (port.IsOpen && port.BytesToRead > 0)
                {
                    var bytesToRead = port.BytesToRead;

                    var rentedBuffer = ArrayPool<byte>.Shared.Rent(bytesToRead);
                    try
                    {
                        var read = port.Read(rentedBuffer, 0, bytesToRead);
                        if (read <= 0)
                        {
                            continue;
                        }

                        var data = new byte[read];
                        Buffer.BlockCopy(rentedBuffer, 0, data, 0, read);
                        var item = new ReceivedItem
                        {
                            Data = data,
                            PortName = port.PortName
                        };
                        if (!channel.Writer.TryWrite(item))
                        {
                            Interlocked.Increment(ref _droppedItems);
                            Trace.WriteLine("Failed to write received item to channel.");
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(rentedBuffer);
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error reading from serial port: {ex}");
            }
        }
    }

    private async Task ProcessQueue(ChannelReader<ReceivedItem> reader, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in reader.ReadAllAsync(cancellationToken))
            {
                var handlers = DataReceived?.GetInvocationList();
                if (handlers is null)
                {
                    continue;
                }

                foreach (var handler in handlers)
                {
                    try
                    {
                        ((EventHandler<DataReceivedEventArgs>)handler).Invoke(
                            this,
                            new DataReceivedEventArgs
                            {
                                Data = item.Data,
                                PortName = item.PortName
                            });
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine($"Subscriber error: {ex}");
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Trace.WriteLine($"Unexpected error in ProcessQueue: {ex}");
        }
    }

    public bool IsOpen
    {
        get
        {
            lock (_sync)
            {
                return _serialPort?.IsOpen == true;
            }
        }
    }

    public bool IsDisposed
    {
        get
        {
            lock (_sync)
            {
                return _disposed;
            }
        }
    }

    public long DroppedItems => Interlocked.Read(ref _droppedItems);

    public void Close()
    {
        CancellationTokenSource? cts = null;
        Task? consumerTask = null;

        _sendSemaphore.Wait();
        try
        {
            lock (_sync)
            {
                var port = _serialPort;
                _serialPort = null;

                if (port is not null)
                {
                    port.DataReceived -= OnDataReceived;
                    try
                    {
                        if (port.IsOpen)
                        {
                            port.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine($"Error closing port: {ex}");
                    }
                    finally
                    {
                        port.Dispose();
                    }
                }

                var writer = _channel?.Writer;
                cts = _cts;
                consumerTask = _consumerTask;
                _channel = null;
                _cts = null;
                _consumerTask = null;
                writer?.TryComplete();
            }
        }
        finally
        {
            _sendSemaphore.Release();
        }

        cts?.Cancel();

        if (consumerTask is not null && consumerTask.Id != Task.CurrentId)
        {
            try
            {
                //consumerTask.GetAwaiter().GetResult();
                consumerTask.Wait(500);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error stopping consumer task: {ex}");
            }
        }

        if (consumerTask is not null && consumerTask.Id == Task.CurrentId)
        {
            _ = consumerTask.ContinueWith(
                _ =>
                {
                    cts?.Dispose();
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        else
        {
            cts?.Dispose();
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        try
        {
            Close();
        }
        finally
        {
            _sendSemaphore.Dispose();
            GC.SuppressFinalize(this);
        }
    }

    public void SendData(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        ObjectDisposedException.ThrowIf(_disposed, this);

        _sendSemaphore.Wait();
        try
        {
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                var port = GetOpenPort();
                port.Write(data, 0, data.Length);
            }
        }
        catch (Exception ex)
        {
            Trace.WriteLine($"Error sending data: {ex}");
            throw;
        }
        finally
        {
            _sendSemaphore.Release();
        }
    }

    public void SendData(string data, Encoding? encoding = null)
    {
        ArgumentNullException.ThrowIfNull(data);
        SendData((encoding ?? Encoding.ASCII).GetBytes(data));
    }

    public async Task SendDataAsync(byte[] data, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _sendSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SerialPort port;
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                port = GetOpenPort();
            }

            await port.BaseStream.WriteAsync(data, cancellationToken).ConfigureAwait(false);
            await port.BaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Trace.WriteLine($"Error sending data async: {ex}");
            throw;
        }
        finally
        {
            _sendSemaphore.Release();
        }
    }

    public Task SendDataAsync(
        string data,
        Encoding? encoding = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);
        return SendDataAsync((encoding ?? Encoding.ASCII).GetBytes(data), cancellationToken);
    }

    private SerialPort GetOpenPort()
    {
        if (_serialPort is null || !_serialPort.IsOpen)
        {
            throw new InvalidOperationException("Serial port is not open.");
        }

        return _serialPort;
    }
}
