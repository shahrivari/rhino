﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino.Util;
using Rhino.IO.Records;
using NLog;
using System.Diagnostics;

namespace Rhino.IO
{
    public class IntermediateFileMerger<InterKey,InterVal>
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        int concurrentFilesCount = 64;
        int maxMemory = 256 * 1024 * 1024;
        string directory;
        
        Guid iD;
        public Guid ID
        {
            get { return iD; }
        }

        string[] files;

        public IntermediateFileMerger(string directory, Guid mapperID)
        {
            this.directory = directory;
            iD = mapperID;
            files=Directory.GetFiles(directory, "*" + mapperID + "*");
            if (files.Length < 1)
                throw new ArgumentException("There is no files to merge!");

        }

        public string Merge(bool keep_files=false)
        {
            int memory_per_file = maxMemory / (concurrentFilesCount+2);
            var fileQ=new Queue<string>(files);
            Stopwatch watch = new Stopwatch();
            long total_records = 0;
            
            while (fileQ.Count > 1)
            {
                watch.Restart();
                var destination_file = new IntermediateFile<InterKey, InterVal>(directory, ID, 2 * memory_per_file);
                var dest = destination_file.FileStream;

                var current_streams = new List<FileStream>();
                for (int i = 0; i < concurrentFilesCount && fileQ.Count > 0; i++)
                    current_streams.Add(new FileStream(fileQ.Dequeue(),FileMode.Open,FileAccess.Read,FileShare.Read,memory_per_file));

                PriorityQueue<InterKey, Stream> priorityQ = new PriorityQueue<InterKey, Stream>();
                
                var stream_len = new Dictionary<Stream, long>();

                foreach(var stream in current_streams)
                {
                    stream_len[stream] = stream.Length;
                    if (stream_len[stream] < sizeof(int))
                        throw new IOException("Malformed intermediate file: The file is too small!");                    
                    var key=IntermediateRecord<InterKey,InterVal>.ReadKey(stream);
                    priorityQ.Enqueue(key, stream);
                }

                logger.Debug("Merging {0} files summing to {1} bytes", current_streams.Count, StringFormatter.HumanReadablePostfixs(stream_len.Values.Sum()));

                var last_key = priorityQ.Peek().Key;
                bool first_time = true;
                while (priorityQ.Count > 0)
                {
                    total_records++;
                    var best=priorityQ.Dequeue();

                    if(!first_time)
                        if (last_key.Equals(best.Key))
                        {
                            dest.WriteByte(1);
                        }
                        else
                            dest.WriteByte(0);
                    last_key = best.Key;
                    first_time = false;

                    destination_file.WriteKey(best.Key);
                    var current_stream = best.Value;
                    var len = IntermediateRecord<InterKey, InterVal>.ReadValueListLength(current_stream);
                    dest.Write(BitConverter.GetBytes(len), 0, sizeof(int));
                    StreamUtils.Copy(current_stream, dest, len - sizeof(byte));
                    current_stream.ReadByte();

                    if (best.Value.Position >= stream_len[current_stream])
                        continue;                    
                    var new_key = IntermediateRecord<InterKey, InterVal>.ReadKey(current_stream);
                    priorityQ.Enqueue(new_key, current_stream);
                }
                dest.WriteByte(0);
                dest.Close();
                fileQ.Enqueue(destination_file.Path);
                foreach (var stream in current_streams)
                {
                    stream.Close();
                    File.Delete(stream.Name);
                }
                watch.Stop();
                logger.Debug("Merged {0} records to {1} in {2}.", StringFormatter.DigitGrouped(total_records), destination_file.Path, watch.Elapsed);
            }

            return fileQ.First();
        }
    }
}
