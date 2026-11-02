using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Protocols
{
    public sealed class ProtocolFrame
    {
        public required ReadOnlyMemory<byte> Raw { get; init; }
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }
}
