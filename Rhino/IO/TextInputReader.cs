using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public abstract class TextInputReader: InputReader
    {
        protected ICollection<string> seperators=null;

        public abstract string ReadRecord();
        public abstract int ReadRecords(out ICollection<string> records, int count);
    }
}
