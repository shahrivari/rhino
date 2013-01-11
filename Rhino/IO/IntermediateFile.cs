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
        
        FileStream keyStream;
        FileStream valStream;

        public IntermediateFile(string directory_path, Guid mapperID)
        {
            this.mapperID = mapperID;
            created_files++;
            string key_file_name = DateTime.Now.ToFileTime().ToString() + "-keys-" + mapperID + "-" + created_files.ToString();
            string val_file_name = DateTime.Now.ToFileTime().ToString() + "-values-" + mapperID + "-" + created_files.ToString();
            valStream = new FileStream(directory_path + "/" + val_file_name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4 * 1024 * 1024);            
            keyStream = new FileStream(directory_path + "/" + key_file_name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4 * 1024 * 1024);            
        }

        public int WriteRecords(IEnumerable<KeyValuePair<InterKey,List<InterValue>>> sorted_pairs)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            long written_bytes=0;
            var key_serializer = Serializer.GetBinarySerializer(typeof(InterKey));
            var value_serializer = Serializer.GetBinarySerializer(typeof(InterValue));
            int values_file_pos = 0;
            MemoryStream key_mem_stream = new MemoryStream();
            MemoryStream val_mem_stream = new MemoryStream();

            foreach (var pair in sorted_pairs)
            {
                var key_bytes = key_serializer.Invoke(pair.Key);
                var key_pos_record = ArrayUtils.Combine(BitConverter.GetBytes(key_bytes.Length), key_bytes, BitConverter.GetBytes(values_file_pos));
                key_mem_stream.Write(key_pos_record, 0, key_pos_record.Length);
                //var key_rec = Serializer.BinarySerializeToSmallRecord(pair.Key);
                //var key_rec_bytes = KeyPositionRecord<InterKey>.GetBytes(pair.Key, values_file_pos, null);
                //key_mem_stream.Write(key_rec_bytes, 0, key_rec_bytes.Length);
                //written_bytes += key_rec_bytes.Length;

                var val_list_bytes = Serializer.BinarySerializeIntermediateList(pair.Value);
                val_mem_stream.Write(val_list_bytes, 0, val_list_bytes.Length);
                values_file_pos += val_list_bytes.Length;
                //var vals_rec = Serializer.BinarySerializeListToSmallRecord(pair.Value);
                //written_bytes += vals_rec.Length;
                //val_mem_stream.Write(vals_rec.Bytes, 0, vals_rec.Length);
                //values_file_pos += vals_rec.Length;
            }

            key_mem_stream.WriteTo(keyStream);
            written_bytes += key_mem_stream.Length;

            val_mem_stream.WriteTo(valStream);
            written_bytes += val_mem_stream.Length;

            watch.Stop();
            logger.Debug("Spilled {0} records summing to {2} bytes to disk in {1}.", StringFormatter.DigitGrouped(sorted_pairs.Count()), watch.Elapsed,written_bytes);

            if (written_bytes > int.MaxValue)
                throw new InvalidCastException("The intermediate file is very huge!");
            return (int)written_bytes;
        }

    }
}
