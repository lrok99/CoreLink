using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core
{
    public abstract class ProtocolParser
    {
        /// <summary>
        /// 解析传入的字节流，将完整的帧通过回调抛出。
        /// 注意：incoming 可能包含半个包或多个包，解析器内部必须维护缓冲区状态。
        /// </summary>
        /// <param name="incoming">原始字节流（一次事件的片段，不保证是完整帧）</param>
        /// <param name="onFrameParsed">
        /// 每解析出一个完整帧时调用。参数为完整帧的只读内存视图（仅在回调期间有效）。
        /// 调用者若需保存，必须自行拷贝。
        /// </param>
        public abstract void Parse(ReadOnlySpan<byte> data, Action<ReadOnlySpan<byte>> onFrameParsed);
    }
}
