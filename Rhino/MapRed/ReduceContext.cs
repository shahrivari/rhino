using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.MapRed
{
    public class ReduceContext
    {
        long emitCount = 0;
        public long EmitCount
        {
            get { return emitCount; }
        }

        StreamWriter writer;

        public ReduceContext(StreamWriter writer)
        {
            this.writer = writer;
        }

        public void Emit(String str)
        {
            writer.WriteLine(str);
            emitCount++;
        }
    }
}
