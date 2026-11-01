using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Transport.Events
{
    /// <summary>
    /// 接收到的原始字节流。注意：一次回调不保证是完整帧，粘包/拆包由协议层处理。
    /// </summary>
    public class DataReceivedEventArgs:EventArgs
    {
        public required ReadOnlyMemory<byte> Data { get; init; }
        public required string SourceName { get; init; }
        public DateTime Timestamp { get; init; } = DateTime.Now;
    }

}
