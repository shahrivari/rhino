using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Rhino.Old
{
    public class SimpleMapReducer<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        ConcurrentMapCombiner<InKey, InValue, InterKey, InterValue> mapper;
        BlockingCollection<Dictionary<InterKey, List<InterValue>>> dicsQ;
        Dictionary<InterKey, List<InterValue>> interList;
        InputRecordReader<InKey, InValue> reader;
        protected Action<InKey, InValue, IMapContext<InterKey, InterValue>> mapFunc;
        long mapCalls = 0;
        long keyCount = 0;

        public SimpleMapReducer(InputRecordReader<InKey, InValue> reader, Action<InKey, InValue, IMapContext<InterKey, InterValue>> mapFunc)
        {
            this.reader = reader;
            this.mapFunc = mapFunc;
        }

        public void Run(int thread_num=0)
        {
            List<KeyValuePair<InKey, InValue>> input = new List<KeyValuePair<InKey, InValue>>();
            interList = new Dictionary<InterKey, List<InterValue>>();
            dicsQ = new BlockingCollection<Dictionary<InterKey, List<InterValue>>>();

            Task t = new Task(() =>
                {
                    while (!dicsQ.IsCompleted)
                    {
                        if (dicsQ.Count == 0)
                        {
                            Thread.Sleep(1);
                            continue;
                        }

                        var dic = dicsQ.Take();
                        foreach (var pair in dic)
                        {
                            if (!interList.ContainsKey(pair.Key))
                                interList[pair.Key] = new List<InterValue>();
                            interList[pair.Key].AddRange(pair.Value);
                        }
                    }
                }
            );
            t.Start();

            int chunk_size = 64*1024;
            while (reader.HasNextRecord())
            {
                input.Clear();
                for(int i=0;i<chunk_size;i++){
                    if(!reader.HasNextRecord())
                        break;
                    input.Add(reader.readNextRecord());
                }
                if (input.Count == 0)
                    continue;
                DateTime t0 = DateTime.Now;
                mapper = new ConcurrentMapCombiner<InKey, InValue, InterKey, InterValue>(mapFunc, input);
                var dics=mapper.Run(thread_num);
                foreach (var dic in dics)
                    dicsQ.Add(dic);

                Interlocked.Add(ref mapCalls, mapper.MapCalls);
                Interlocked.Add(ref keyCount, mapper.KeyCount);
                if ((DateTime.Now - t0).TotalMilliseconds < 50)
                {
                    chunk_size *= 2;
                    //Console.WriteLine(chunk_size);
                }
            }
            dicsQ.CompleteAdding();

            t.Wait();
        }

        
        
    }
}
