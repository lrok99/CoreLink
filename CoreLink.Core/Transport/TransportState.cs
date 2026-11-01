using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Transport
{
    /// <summary>
    /// 传输状态枚举。
    /// </summary>
    public enum TransportState
    {
        Closed,
        Opening,
        Open,
        Closing,
        Faulted,
  
    }
}
