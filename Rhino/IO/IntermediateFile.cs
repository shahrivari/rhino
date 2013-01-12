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

        Guid mapperID;
        string path;
        public string Path
        {
            get { return path; }
        }
        
        private static Logger logger = LogManager.GetCurrentClassLogger();
        
        FileStream fileStream;
        //FileStream valStream;

        public IntermediateFile(string directory_path, Guid mapperID)
        {
            this.mapperID = mapperID;
            created_files++;
            string file_name = DateTime.Now.ToFileTime().ToString() + "-" + mapperID + "-" + created_files.ToString();
            //string val_file_name = DateTime.Now.ToFileTime().ToString() + "-values-" + mapperID + "-" + created_files.ToString();
            fileStream = new FileStream(directory_path + "/" + file_name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4 * 1024 * 1024);            
            //keyStream = new FileStream(directory_path + "/" + key_file_name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4 * 1024 * 1024);            
        }

        public int WriteRecords(IEnumerable<KeyValuePair<InterKey,List<InterValue>>> sorted_pairs)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            long written_bytes=0;

            foreach (var pair in sorted_pairs)
            {
                var bytes=IntermediateRecord<InterKey,InterValue>.GetIntermediateRecordBytes(pair.Key,pair.Value);
                fileStream.Write(bytes, 0, bytes.Length);
                written_bytes += bytes.Length;
            }

            watch.Stop();
            logger.Debug("Spilled {0} records summing to {2} bytes to disk in {1}.", StringFormatter.DigitGrouped(sorted_pairs.Count()), watch.Elapsed,written_bytes);

            if (written_bytes > int.MaxValue)
                throw new InvalidCastException("The intermediate file is very huge!");
            return (int)written_bytes;
        }

    }
}
