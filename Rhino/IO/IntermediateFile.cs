using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using NLog;

using Rhino.Util;
using Rhino.IO.Records;


namespace Rhino.IO
{
    public class IntermediateFile <InterKey,InterValue>
    {
        static int created_files = 0;

        //Guid mapperID;
        string path;
        public string Path
        {
            get { return path; }
        }
        
        private static Logger logger = LogManager.GetCurrentClassLogger();

        FileStream fileStream;

        public FileStream FileStream
        {
            get { return fileStream; }
        }

        public IntermediateFile(string directory_path, Guid mapperID, int buffer_size = 4 * 1024 * 1024)
        {
            //this.mapperID = mapperID;
            created_files++;
            string file_name = DateTime.Now.ToFileTime().ToString() + "-" + mapperID + "-" + created_files.ToString();
            path = directory_path + "/" + file_name;
            fileStream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, buffer_size);            
        }

        public void Write(byte[] array)
        {
            fileStream.Write(array, 0, array.Length);
        }

        public int WriteKey(InterKey key)
        {
            var key_bytes = Serialization.GetBinarySerializer(typeof(InterKey)).Invoke(key);
            fileStream.Write(BitConverter.GetBytes(key_bytes.Length), 0, sizeof(int));
            fileStream.Write(key_bytes, 0, key_bytes.Length);
            return key_bytes.Length + sizeof(int);
        }


        public long WriteRecords(IEnumerable<KeyValuePair<InterKey,List<InterValue>>> sorted_pairs)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            long written_bytes=0;
            

            foreach (var pair in sorted_pairs)
            {
                var record_bytes=IntermediateRecord<InterKey,InterValue>.GetIntermediateRecordBytes(pair.Key,pair.Value);
                foreach (var bytes in record_bytes)
                {
                    fileStream.Write(bytes, 0, bytes.Length);
                    written_bytes += bytes.Length;
                }
            }

            watch.Stop();
            logger.Debug("Spilled {0} records summing to {2} bytes to disk in {1}.", StringFormatter.DigitGrouped(sorted_pairs.Count()), watch.Elapsed, StringFormatter.HumanReadablePostfixs(written_bytes));

            //if (written_bytes > int.MaxValue)
            //    throw new InvalidCastException("The intermediate file is very huge!");
            return written_bytes;
        }

        public void Close()
        {
            fileStream.Close();
        }

    }
}
