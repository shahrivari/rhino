using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.Old
{
    public class LineReader : InputRecordReader<string, string>
    {
        StringReader reader;
        string line="";

        public LineReader(string str)
        {
            reader=new StringReader(str);
            line = reader.ReadLine();
        }

        public KeyValuePair<String, String> readNextRecord() {
            if (line == null)
                throw new Exception();
            var old_line = line;
            line = reader.ReadLine();
            return new KeyValuePair<string, string>(old_line, old_line);
        }

        public bool HasNextRecord()
        {
            if (line == null)
                return false;
            return true;
        }
    }
}
