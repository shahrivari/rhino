using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino.Util;
using Rhino.IO.Records;

namespace Rhino.IO
{
    public class IntermediateFileMerger<InterKey,InterVal>
    {
        int concurrentFilesCount = 20;
        int maxMemory = 512 * 1024 * 1024;

        string[] files;

        public IntermediateFileMerger(string directory, Guid mapperID)
        {
            files=Directory.GetFiles(directory, "*" + mapperID + "*");
            if (files.Length < 1)
                throw new ArgumentException("There is no files to merge!");

        }

        public string Merge()
        {
            int memory_per_file = maxMemory / (concurrentFilesCount-2);
            var fileQ=new Queue<string>(files);
            var dest = new FileStream(@"z:\\pashm\out.dat",FileMode.Create,FileAccess.Write,FileShare.Read,2*memory_per_file);
            while (fileQ.Count > 1)
            {
                //var current_files=new List<string>();
                var current_streams = new List<FileStream>();
                for (int i = 0; i < concurrentFilesCount && fileQ.Count > 0; i++)
                    current_streams.Add(new FileStream(fileQ.Dequeue(),FileMode.Open,FileAccess.Read,FileShare.Read,memory_per_file));

                PriorityQueue<InterKey, Stream> priorityQ = new PriorityQueue<InterKey, Stream>(concurrentFilesCount);
                
                var stream_len = new Dictionary<Stream, long>();

                // if the stream is zero size?????????
                foreach(var stream in current_streams)
                {
                    var key=IntermediateRecord<InterKey,InterVal>.ReadKey(stream);
                    priorityQ.Enqueue(key, stream);
                    stream_len[stream] = stream.Length;
                }

                int x = 0;
                bool first = true;
                InterKey last_key=default(InterKey);
                while (priorityQ.Count > 0)
                {
                    var best=priorityQ.Dequeue();
                    if (first)
                    {
                        last_key = best.Key;
                        first = false;
                    }
                    else
                        if (!best.Key.Equals(last_key))
                        {
                            last_key = best.Key;
                            var key_bytes = Serialization.GetBinarySerializer(typeof(InterKey)).Invoke(best.Key);
                            dest.Write(BitConverter.GetBytes(key_bytes.Length),0,sizeof(int));
                            dest.Write(key_bytes, 0, key_bytes.Length);
                        }

                    var current_stream = best.Value;
                    var len = IntermediateRecord<InterKey, InterVal>.ReadValueListLength(current_stream);
                    dest.Write(BitConverter.GetBytes(len), 0, sizeof(long));
                    StreamUtils.Copy(current_stream, dest, len);

                    if (best.Value.Position >= stream_len[current_stream])
                        continue;
                    
                    var new_key = IntermediateRecord<InterKey, InterVal>.ReadKey(current_stream);
                    priorityQ.Enqueue(new_key, current_stream);
                    if (x++ % 100000 == 0)
                        Console.WriteLine(x);
                }

            }

            return null;
            return fileQ.First();
        }
    }
}
