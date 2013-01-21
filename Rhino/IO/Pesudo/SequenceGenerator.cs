using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO.Pesudo
{
    public class IntegerSequenceGenerator: TextInputReader
    {
        long from = 0;
        long to = 0;
        long step = 1;
        long current = 0;
        
        public override long Length
        {
            get { return (to - from)/step; }
        }

        public override long Position
        {
            get { return (current - from) / step; }
        }

        public IntegerSequenceGenerator(long from, long to, long step=1)
        {
            if (to < from)
                throw new InvalidOperationException("End must be greater than beginning.");
            if(step<1)
                throw new InvalidOperationException("Step must be more than 1.");
            
            this.from = from;
            this.to = to;
            this.step = step;
            current = from;
        }

        public override string ReadRecord()
        {
            if (current >= to)
                return null;
            var s= current.ToString();
            current++;
            return s;            
        }

        public override List<string> ReadRecords(int count)
        {
            List<String> list=new List<string>(count);
            for (int i = 0; i < count; i++)
            {
                var s = ReadRecord();
                if (s == null)
                    break;
                list.Add(s);
            }
            return list;
        }

        public override int ReadChunk(out InputTextCunk chunk, int max_char_count)
        {
            List<String> list = new List<string>(max_char_count);
            int count = 0;
            for (int i = 0; i < max_char_count; i++)
            {
                var s = ReadRecord();
                if (s == null)
                    break;
                count += s.Length;
                list.Add(s);
                if (count > max_char_count)
                    break;
            }

            chunk = new InputTextCunk(list, count);
            return count;
        }

    }
}
