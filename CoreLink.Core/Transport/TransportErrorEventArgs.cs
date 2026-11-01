using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Transport
{
    public class TransportErrorEventArgs:EventArgs
    {
        public required Exception Exception { get; init; }
        public required string SourceName { get; init; }

        public bool IsFatal { get; init; } = false;

    }
}
