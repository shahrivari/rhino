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

        public override List<string> ReadRecords(int count)
        {
            var records=new List<string>();
            for (int i = 0; i < count; i++)
            {
                string next_record=ReadRecord();
                if (next_record == null)
                    break;
                else
                    records.Add(next_record);
            }

            return records;
        }

        public override int ReadChunk(out InputTextCunk chunk, int max_char_count)
        {
            var records = new List<string>();
            int char_count = 0;
            while(true)
            {
                string next_record = ReadRecord();
                if (next_record == null)
                    break;
                else
                    records.Add(next_record);
                
                char_count += next_record.Length;
                if (char_count >= max_char_count)
                    break;

            }

            chunk = new InputTextCunk(records, char_count);
            return char_count;
        }

    }
}
