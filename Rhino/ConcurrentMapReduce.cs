using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Text;

namespace Rhino
{
    public class ConcurrentMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        object lockObject=new object();
        List<Dictionary<InterKey, List<InterValue>>> dics = new List<Dictionary<InterKey, List<InterValue>>>();
        BlockingCollection<List<KeyValuePair<InKey, InValue>>> queue = new BlockingCollection<List<KeyValuePair<InKey, InValue>>>(1024);
        
        InputRecordReader<InKey, InValue> input;

        public ConcurrentMapReduce(Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc,
                               Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc
                               , InputRecordReader<InKey, InValue> input)
        {
            this.mapFunc = mapFunc;
            this.reduceFunc = reduceFunc;
            this.input = input;          
        }

        void produce()
        {
            int chunkSize = 10 * 1024;
            var chunk=new List<KeyValuePair<InKey,InValue>>(chunkSize);
            
            while (input.HasNextRecord())
            {
                var record = input.readNextRecord();
                chunk.Add(record);
                if(chunk.Count>=chunkSize)
                {
                    queue.Add(chunk);
                    chunk = new List<KeyValuePair<InKey, InValue>>();
                }                
            }
            if (chunk.Count > 0)
                queue.Add(chunk);
            queue.CompleteAdding();
        }

        Thread[] threads = new Thread[Environment.ProcessorCount];
        
        void consume()
        {
            while (!queue.IsCompleted)
            {
                var list=new List<KeyValuePair<InKey,InValue>>();
                bool try_res=queue.TryTake(out list, 1);
                if (try_res == false)
                {
                    //Console.WriteLine("OOPS");
                    Thread.Sleep(10);
                    continue;
                }

                var dic = new Dictionary<InterKey, List<InterValue>>();
                var context = new MapContext<InterKey, InterValue>(dic);
                foreach (var x in list)
                    
                    mapFunc.Invoke(x.Key, x.Value, context);
                lock (lockObject)
                {
                    dics.Add(dic);
                }
            }
        }

        public void Run()
        {
            Thread producer = new Thread(new ThreadStart(produce));
            producer.Start();
            for (int i = 0; i < threads.Length; i++)
            {
                threads[i] = new Thread(new ThreadStart(consume));
                threads[i].Start();
            }

            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
        }




    }
}
