using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public class InputTextCunk
    {
        private List<string> records;
        public List<string> Records
        {
            get { return records; }
        }

        private int charCount;
        public int CharCount
        {
            get { return charCount; }
        }

        public InputTextCunk(List<string> records, int char_count)
        {
            this.records = records;
            charCount = char_count;
        }
    }
}
