using System;
using System.Collections.Generic;
using System.Text;
using CoreLink.Core.Transport.Common;

namespace CoreLink.Core.Transport
{
    /// <summary>
    /// 传输状态变化事件参数。
    /// </summary>
    public class TransportStateChangedEventArgs
    {
        public required TransportState OldState { get; init; }
        public required TransportState NewState { get; init; }
        public required string SourceName { get; init; }
        public string Reason { get; set; }
    }
}
