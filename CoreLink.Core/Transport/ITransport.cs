using CoreLink.Core.Transport.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Transport
{
    public interface ITransport:IDisposable,IAsyncDisposable
    {
        /// <summary>传输实例名称（如 "COM3"、"192.168.1.10:8080"）</summary>
        string Name { get; }
        /// <summary>当前是否已打开</summary>
        bool IsOpen { get; }
        /// <summary>收到原始字节流时触发。注意：一次事件不等于一个完整数据包。</summary>
        event EventHandler<DataReceivedEventArgs>? DataReceived;
        /// <summary>传输层发生错误时触发（如拔线、超时、硬件异常）</summary>
        event EventHandler<Events.ErrorEventArgs>? ErrorOccurred;
        /// <summary>状态变化时触发（打开、关闭、异常断开）</summary>
        event EventHandler<TransportStateChangedEventArgs>? StateChanged;

        Task OpenAsync(CancellationToken cancellationToken);
        Task CloseAsync(CancellationToken cancellationToken);
        Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

        TransportState State { get; }

    }
}
