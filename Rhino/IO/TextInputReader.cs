using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public abstract class TextInputReader
    {
        protected ICollection<string> seperators=null;

        public abstract long Length { get; }
        public abstract long Position {get; }


        public abstract string ReadRecord();
        public abstract List<string> ReadRecords(int count);
        public abstract int ReadChunk(out InputTextCunk chunk, int max_char_count);
    }
}
