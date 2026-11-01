using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Transport.Common
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
