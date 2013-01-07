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
using Rhino.Util;

namespace Rhino.MapRed
{
    public class TextMapper<InterKey, InterValue>
    {
        protected Action<string, MapContext<InterKey, InterValue>> mapFunc = null;
        protected Func<List<InterValue>, InterValue> combineFunc = null;
        protected TextInputReader reader;
        private Object diskLock=new object();
        
        protected int maxChunkSize = 4 * 1024 * 1024;
        TimeSpan minWorkPeriod = new TimeSpan(0, 0, 0, 0, 100);
        TimeSpan maxWorkPeriod = new TimeSpan(0, 0, 0, 0, 300);
        int maxCharsToMap = 32 * 1024 * 1024;
        int maxIntermediatePairsToSpill = 1 * 1024 * 1024;

        private static Logger logger = LogManager.GetCurrentClassLogger();
        
        TextMapperInfo mapperInfo = new TextMapperInfo();
        public TextMapperInfo MapperInfo
        {
            get { return mapperInfo; }
        }

        //ConcurrentBag<Dictionary<InterKey, List<InterValue>>> dics=new ConcurrentBag<Dictionary<InterKey,List<InterValue>>>();
        BlockingCollection<Dictionary<InterKey, List<InterValue>>> dicsQ = new BlockingCollection<Dictionary<InterKey, List<InterValue>>>(32);

        BlockingCollection<InputTextCunk> inputQ = new BlockingCollection<InputTextCunk>(4);

        public TextMapper(TextInputReader reader, Action<string, MapContext<InterKey, InterValue>> map_func, Func<List<InterValue>, InterValue> combine_func = null)
        {
            this.reader = reader;
            mapFunc = map_func;
            combineFunc = combine_func;
            logger.Info("Mapper created.");
        }

        private void readInput()
        {
            while (true)
            {
                InputTextCunk input_chunk;
                int char_count = 0;
                lock (diskLock)
                {
                    char_count = reader.ReadChunk(out input_chunk, maxChunkSize);

                }
                if (char_count == 0)
                    break;
                inputQ.Add(input_chunk);
                logger.Debug("Read a chunk: {0} records and {1} chars. InputQ count is {2}", StringFormatter.HumanReadablePostfixs(input_chunk.CharCount), StringFormatter.HumanReadablePostfixs(char_count), inputQ.Count);
            }
            inputQ.CompleteAdding();
        }

        private void combine()
        {            
            var overall_dic = new Dictionary<InterKey, List<InterValue>>(1024);
            int overall_dic_pair_count = 0;
            Stopwatch watch = new Stopwatch();

            while (!dicsQ.IsCompleted)
            {
                var dic = dicsQ.Take();
                foreach(var pair in dic)
                {
                    List<InterValue> intermediate_list=null;
                    if (!overall_dic.TryGetValue(pair.Key, out intermediate_list))
                    {
                         intermediate_list= new List<InterValue>();
                         overall_dic[pair.Key]=intermediate_list;
                    }

                    var new_list = pair.Value;
                    intermediate_list.AddRange(new_list);
                    overall_dic_pair_count += new_list.Count;

                    if (intermediate_list.Count > 4 && combineFunc != null)
                    {
                        overall_dic[pair.Key] = new List<InterValue>() { combineFunc.Invoke(intermediate_list) };
                        overall_dic_pair_count -= intermediate_list.Count - 1;
                    }
                }

                if (overall_dic_pair_count > maxIntermediatePairsToSpill)
                {
                    watch.Restart();                    
                    //var sorted_pairs = from t in overall_dic orderby t.Key select t;
                    var sorted_pairs = overall_dic.AsParallel().OrderBy(t => t.Key).ToArray();
                    overall_dic = new Dictionary<InterKey, List<InterValue>>();
                    overall_dic_pair_count = 0;                    
                    sorted_pairs.First();
                    
                    watch.Stop();
                    logger.Debug("Spilled {0} records to disk in {1}.", sorted_pairs.Count(),watch.Elapsed);
                }
            }
       
        }

        private void consumeInput(int thread_num = 0)
        {
            Stopwatch watch = new Stopwatch();
            while (!inputQ.IsCompleted)
            {
                
                var chunk=inputQ.Take();
                var input_records = chunk.Records;
                var char_count = chunk.CharCount;
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
                    Interlocked.Add(ref mapperInfo.MapEmits, context.EmitCount);
                });
                watch.Stop();

                mapperInfo.ProcessedRecords += input_records.Count;
                mapperInfo.ProcessedChars+= char_count;

                if (watch.Elapsed > maxWorkPeriod)
                    maxChunkSize = Math.Min(maxChunkSize / 2,maxCharsToMap);
                if (watch.Elapsed < minWorkPeriod)
                    maxChunkSize = Math.Min(maxChunkSize * 2,maxCharsToMap);
                
                logger.Debug("Mapped a chunk with {0} chars in {1}", StringFormatter.HumanReadablePostfixs(char_count), watch.Elapsed);

                foreach (var dic in dics)
                    dicsQ.Add(dic);
            }

            dicsQ.CompleteAdding();
            logger.Info("Mapper processed {0} records that sums to {1} chars.", StringFormatter.HumanReadablePostfixs(mapperInfo.ProcessedRecords), StringFormatter.HumanReadablePostfixs(mapperInfo.ProcessedChars));
        }
        
        public void Run()
        {
            Thread input_reader_thread = new Thread(new ThreadStart(readInput));
            input_reader_thread.Start();
            Thread combiner_thread = new Thread(new ThreadStart(combine));
            combiner_thread.Start();            

            //readInput();
            consumeInput();
            input_reader_thread.Join();
            combiner_thread.Join();
        }
    }
}
