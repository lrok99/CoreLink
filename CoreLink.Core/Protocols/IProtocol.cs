using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Protocols
{
    public interface IProtocol
    {
        string Name { get; }
        /// <summary>
        /// 每解析出一帧时调用。回调抛异常会中断当前 Parse 调用。
        /// 订阅者若需长期持有 Raw，必须自行 .ToArray() 拷贝。
        /// </summary>
        /// <param name="raw"></param>
        /// <param name="onFrameParsed"></param>
        void ParseFrame(ReadOnlyMemory<byte> raw, Action<ProtocolFrame> onFrameParsed);
        /// <summary>
        /// 重置内部缓冲。重连/切换协议时由上层调用。
        /// </summary>
        void Reset();
    }
}
