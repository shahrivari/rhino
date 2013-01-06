using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Diagnostics;
using NLog;

using Rhino.IO;

namespace Rhino.MapRed
{
    public class TextMapper<InterKey, InterValue>
    {
        protected Action<string, MapContext<InterKey, InterValue>> mapFunc;
        protected TextInputReader reader;
        private Object diskLock=new object();
        protected int maxChunkSize = 4 * 1024 * 1024;
        TimeSpan minWorkPeriod = new TimeSpan(0, 0, 0, 0, 100);
        TimeSpan maxWorkPeriod = new TimeSpan(0, 0, 0, 0, 300);

        private static Logger logger = LogManager.GetCurrentClassLogger();
        
        //ConcurrentBag<Dictionary<InterKey, List<InterValue>>> dics=new ConcurrentBag<Dictionary<InterKey,List<InterValue>>>();

        BlockingCollection<List<string>> inputQ = new BlockingCollection<List<string>>(4);


        long mapEmits = 0;
        public long MapEmits
        {
            get { return mapEmits; }
        }

        
        public TextMapper(TextInputReader reader, Action<string, MapContext<InterKey, InterValue>> map_func)
        {
            this.reader = reader;
            mapFunc = map_func;
            logger.Error("WOOOOOOOOOOOOOOORKS!");
            //Console.ReadLine();
        }

        private void readInput()
        {
            while (true)
            {
                List<string> input_records;
                int char_count = 0;
                lock (diskLock)
                {
                    char_count = reader.ReadRecordsChars(out input_records, maxChunkSize);

                }
                if (char_count == 0)
                    break;
                inputQ.Add(input_records);
                //Console.WriteLine("Read Chunk!");
            }
            inputQ.CompleteAdding();
        }

        private void consumeInput(int thread_num = 0)
        {
            Stopwatch watch = new Stopwatch();
            while (!inputQ.IsCompleted)
            {
                
                var input_records = inputQ.Take();
                //Console.WriteLine("Got Chunk!");

                watch.Restart();
                var dics = new ConcurrentBag<Dictionary<InterKey, List<InterValue>>>();
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
                    Interlocked.Add(ref mapEmits, context.EmitCount);
                });
                watch.Stop();
                if (watch.Elapsed > maxWorkPeriod)
                    maxChunkSize = maxChunkSize / 2;
                if (watch.Elapsed < minWorkPeriod)
                    maxChunkSize = maxChunkSize * 2;
                Console.WriteLine(maxChunkSize+"   "+watch.Elapsed);
            }
        }
        
        public void Run()
        {
            Thread t = new Thread(new ThreadStart(readInput));
            t.Start();            
            //readInput();
            consumeInput();
            t.Join();
        }
    }
}
