using System;
using System.Collections.Generic;
using System.Text;

namespace CoreLink.Core.Protocols
{
    public class PassThroughProtocol : IProtocol
    {
        public string Name => "PassThrough";

        public void ParseFrame(ReadOnlyMemory<byte> raw, Action<ProtocolFrame> onFrameParsed)
        {
            if(raw.Length == 0)
            {
                return;
            }
            onFrameParsed(new ProtocolFrame { Raw = raw });
        }

        public void Reset()
        {
        }
    }
}
