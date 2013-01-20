using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public class RandomIntegerReader:TextInputReader
    {
        int max = 0;
        int consumed = 0;
        Random rand = new Random();
        
        public override long Length
        {
            get { return max; }
        }

        public override long Position
        {
            get { return consumed; }
        }

        public RandomIntegerReader(int count)
        {
            max = count;
        }

        public override string ReadRecord()
        {
            if (consumed >= max)
                return null;
            consumed++;
            return consumed.ToString();
            //return rand.Next().ToString();            
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
