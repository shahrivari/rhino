using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using NLog;

using Rhino.Util;

namespace Rhino.IO
{
    public class IntermediateFile <InterKey,InterValue>
    {
        static int created_files = 0;
        string path;
        public string Path
        {
            get { return path; }
        }
        
        private static Logger logger = LogManager.GetCurrentClassLogger();
        FileStream stream;

        public IntermediateFile(string directory_path)
        {
            created_files++;
            string file_name = DateTime.Now.ToFileTime().ToString() + "-" + created_files.ToString();
            path = directory_path + "/" + file_name;
            stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4 * 1024 * 1024);            
        }

        public int WriteRecords(IEnumerable<KeyValuePair<InterKey,List<InterValue>>> sorted_pairs)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            int written_bytes=0;
            var key_serializer = Serializer.GetBinarySerializer(typeof(InterKey));
            var value_serializer = Serializer.GetBinarySerializer(typeof(InterValue));
            foreach (var pair in sorted_pairs)
            {
                var key_rec = Serializer.BinarySerializeToSmallRecord(pair.Key);
                written_bytes+=key_rec.Length;
                stream.Write(key_rec.Bytes, 0, key_rec.Length);
                var vals_rec = Serializer.BinarySerializeListToSmallRecord(pair.Value);
                written_bytes += vals_rec.Length;
                stream.Write(vals_rec.Bytes, 0, vals_rec.Length);
            }

            watch.Stop();
            logger.Debug("Spilled {0} records summing to {2} bytes to disk in {1}.", StringFormatter.DigitGrouped(sorted_pairs.Count()), watch.Elapsed,written_bytes);

            return written_bytes;
        }

    }
}
