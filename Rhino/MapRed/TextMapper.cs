using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;

using Rhino.IO;

namespace Rhino.MapRed
{
    public class TextMapper<InterKey, InterValue>
    {
        protected Action<string, MapContext<InterKey, InterValue>> mapFunc;
        protected TextInputReader reader;
        private Object diskLock=new object();
        protected int maxChunkSize = 1 * 1024 * 1024;
        ConcurrentBag<Dictionary<InterKey, List<InterValue>>> dics;

        long mapCalls = 0;
        public long MapCalls
        {
            get { return mapCalls; }
        }


        public TextMapper(TextInputReader reader, Action<string, MapContext<InterKey, InterValue>> map_func)
        {
            this.reader = reader;
            mapFunc = map_func;
        }

        public void Run(int thread_num = 0)
        {
            while (true)
            {
                List<string> input_records;
                lock (diskLock)
                {
                    var char_count = reader.ReadRecordsChars(out input_records, maxChunkSize);
                    if (char_count == 0)
                        break;
                }

                dics = null;
                dics = new ConcurrentBag<Dictionary<InterKey, List<InterValue>>>();

                ParallelOptions option = new ParallelOptions();
                if (thread_num == 0)
                    option.MaxDegreeOfParallelism = Environment.ProcessorCount * 2;
                else
                    option.MaxDegreeOfParallelism = thread_num;
                Parallel.ForEach(Partitioner.Create(0, input_records.Count), option, (range) =>
                {
                    var dic = new Dictionary<InterKey, List<InterValue>>();
                    var context = new MapContext<InterKey, InterValue>(dic);
                    for (int i = range.Item1; i < range.Item2; i++)
                        mapFunc.Invoke(input_records[i], context);
                    dics.Add(dic);
                    Interlocked.Add(ref mapCalls, context.EmitCount);
                });


                                
            }
 
        }
    }
}
