using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.IO
{
    public class TextFileLineByLineReader: TextInputReader
    {
        private string filePath;
        private StreamReader reader;


        long maxLines = -1;

        long lineNumber = 0;
        bool prepenLineNumber=false;
        public bool PrepenLineNumber
        {
          get { return prepenLineNumber; }
          set { prepenLineNumber = value; }
        }

        public override long Length
        {
            get 
            {
                return reader.BaseStream.Length;
            }
        }

        public override long Position
        {
            get
            {
                return reader.BaseStream.Position;
            }
        }

        public TextFileLineByLineReader(string file_path, long skip_lines=-1, long max_lines=-1)
        {
            maxLines = max_lines;
            filePath = file_path;
            reader = File.OpenText(filePath);
            if (skip_lines != -1)
                for (long i = 0; i < skip_lines; i++)
                {
                    reader.ReadLine();
                    lineNumber++;
                }
        }

        public override string ReadRecord()
        {
            if (maxLines != -1 && maxLines <= lineNumber)
                return null;
            var line=reader.ReadLine();
            if (line == null)
                return null;
            if(prepenLineNumber)
                line = lineNumber.ToString() + " " + line;
            lineNumber++;
            
            return line;
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
