using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.IO
{
    public class TextFileInputReader: TextInputReader
    {
        private string filePath;
        private StreamReader reader;

        private bool lineReader
        {
            get
            {
                if (seperators == null)
                    return true;
                return false; 
            }
        }

        public TextFileInputReader(string file_path)
        {
            filePath = file_path;
            reader = File.OpenText(filePath);
        }

        public override string ReadRecord()
        {
            if (lineReader)
                return reader.ReadLine();
            
            return null;                        
        }

        public override int ReadRecords(out ICollection<string> records, int count)
        {
            records=new List<string>();
            for (int i = 0; i < count; i++)
            {
                string next_record=ReadRecord();
                if (next_record == null)
                    break;
                else
                    records.Add(next_record);
            }

            return records.Count;
        }

    }
}
