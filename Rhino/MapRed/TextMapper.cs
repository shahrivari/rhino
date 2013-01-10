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

        private string tempDirectory = @"z:\pashm";
        
        protected int maxChunkSize = 4 * 1024 * 1024;
        TimeSpan minWorkPeriod = new TimeSpan(0, 0, 0, 0, 100);
        TimeSpan maxWorkPeriod = new TimeSpan(0, 0, 0, 0, 300);
        int maxCharsToMap = 32 * 1024 * 1024;
        int maxIntermediatePairsToSpill = 512 * 1024;
        int maxIntermediateFileSize = 256 * 1024 * 1024;

        Dictionary<InterKey, List<InterValue>> cumulativeDictionary = new Dictionary<InterKey, List<InterValue>>(1024);
        int cumulativeDicPairCount = 0;


        private static Logger logger = LogManager.GetCurrentClassLogger();
        
        TextMapperInfo mapperInfo = new TextMapperInfo();
        public TextMapperInfo MapperInfo
        {
            get { return mapperInfo; }
        }

        BlockingCollection<Dictionary<InterKey, List<InterValue>>> dicsQ = new BlockingCollection<Dictionary<InterKey, List<InterValue>>>(100);
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
                logger.Debug("Read a chunk: {0} records and {1} chars. InputQ count is {2}", StringFormatter.DigitGrouped(input_chunk.CharCount), StringFormatter.HumanReadablePostfixs(char_count), inputQ.Count);
            }
            inputQ.CompleteAdding();
        }

        private void doSpillIfNeeded(bool final_spill=false)
        {            
            if (cumulativeDictionary.Count>0 && (cumulativeDicPairCount > maxIntermediatePairsToSpill || final_spill))
            {
                Stopwatch watch = new Stopwatch();
                watch.Restart();
                var sorted_pairs = cumulativeDictionary.AsParallel().OrderBy(t => t.Key).ToArray();
                cumulativeDictionary = new Dictionary<InterKey, List<InterValue>>();
                cumulativeDicPairCount = 0;
                mapperInfo.SpilledRecords += sorted_pairs.Count();
                watch.Stop();
                logger.Debug("Sorted {0} records in {1}.", StringFormatter.DigitGrouped(sorted_pairs.Count()), watch.Elapsed);
                IntermediateFile<InterKey, InterValue> inter_file = new IntermediateFile<InterKey, InterValue>(tempDirectory);
                int written_bytes = 0;
                lock (diskLock)
                {
                    written_bytes = inter_file.WriteRecords(sorted_pairs);
                }
                mapperInfo.SpilledBytes += written_bytes;
                if (!final_spill)
                {
                    if (written_bytes < maxIntermediateFileSize)
                    {
                        maxIntermediatePairsToSpill = (int)(maxIntermediatePairsToSpill * (double)(maxIntermediateFileSize) / written_bytes);
                        logger.Debug("maxIntermediatePairsToSpill was set to {0} records.", StringFormatter.DigitGrouped(maxIntermediatePairsToSpill));
                    }
                    if (written_bytes > 1.5 * maxIntermediateFileSize)
                    {
                        maxIntermediatePairsToSpill /= 2;
                        logger.Debug("maxIntermediatePairsToSpill was set to {0} records.", StringFormatter.DigitGrouped(maxIntermediatePairsToSpill));
                    }
                }
            }
        }

        private void doCombine(Dictionary<InterKey, List<InterValue>> dic)
        {
            foreach (var pair in dic)
            {
                List<InterValue> intermediate_list = null;
                if (!cumulativeDictionary.TryGetValue(pair.Key, out intermediate_list))
                {
                    intermediate_list = new List<InterValue>();
                    cumulativeDictionary[pair.Key] = intermediate_list;
                }

                var new_list = pair.Value;
                intermediate_list.AddRange(new_list);
                cumulativeDicPairCount += new_list.Count;

                if (intermediate_list.Count > 4 && combineFunc != null)
                {
                    cumulativeDictionary[pair.Key] = new List<InterValue>() { combineFunc.Invoke(intermediate_list) };
                    cumulativeDicPairCount -= intermediate_list.Count - 1;
                }
            }
        }

        private void combine()
        {            
            Stopwatch watch = new Stopwatch();

            while (!dicsQ.IsCompleted)
            {
                var dic = dicsQ.Take();
                doCombine(dic);
                doSpillIfNeeded();
            }
            doSpillIfNeeded(true);
        }


        private IEnumerable<Dictionary<InterKey, List<InterValue>>> doMap(InputTextCunk chunk, int thread_num=0)
        {
            Stopwatch watch = new Stopwatch();
            var input_records = chunk.Records;
            var char_count = chunk.CharCount;

            watch.Restart();
            var dics = new ConcurrentBag<Dictionary<InterKey, List<InterValue>>>();
            ParallelOptions option = new ParallelOptions();
            if (thread_num != 0)
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
            mapperInfo.ProcessedChars += char_count;

            if (watch.Elapsed > maxWorkPeriod)
                maxChunkSize = Math.Min(maxChunkSize / 2, maxCharsToMap);
            if (watch.Elapsed < minWorkPeriod)
                maxChunkSize = Math.Min(maxChunkSize * 2, maxCharsToMap);

            logger.Debug("Mapped a chunk with {0} chars in {1}", StringFormatter.DigitGrouped(char_count), watch.Elapsed);
            return dics;
        }

        private void consumeInput(int thread_num = 0)
        {
            while (!inputQ.IsCompleted)
            {
                
                var chunk=inputQ.Take();
                var dics = doMap(chunk, thread_num);
                foreach (var dic in dics)
                    dicsQ.Add(dic);
            }

            dicsQ.CompleteAdding();
            logger.Info("Mapper processed {0} records that sums to {1} chars.", StringFormatter.DigitGrouped(mapperInfo.ProcessedRecords), StringFormatter.HumanReadablePostfixs(mapperInfo.ProcessedChars));
        }

        Stopwatch mapperWatch = new Stopwatch();

        private void logCompletionInfo()
        {
            logger.Info("Mapper finished the job in {0}!", mapperWatch.Elapsed);
            logger.Info("Mapper mapped {0} records that sums to {1} chars.", StringFormatter.DigitGrouped(mapperInfo.ProcessedRecords), StringFormatter.HumanReadablePostfixs(mapperInfo.ProcessedChars));
            logger.Info("Mapper emmited {0} pairs.", StringFormatter.DigitGrouped(mapperInfo.MapEmits));
            logger.Info("Mapper spilled {0} records that sums to {1} bytes.", StringFormatter.DigitGrouped(mapperInfo.SpilledRecords), StringFormatter.HumanReadablePostfixs(mapperInfo.SpilledBytes));
        }

        
        public void Run()
        {            
            mapperWatch.Restart();

            Thread input_reader_thread = new Thread(new ThreadStart(readInput));
            input_reader_thread.Start();
            Thread combiner_thread = new Thread(new ThreadStart(combine));
            combiner_thread.Start();            

            //readInput();
            consumeInput();
            input_reader_thread.Join();
            combiner_thread.Join();
            mapperWatch.Stop();
            logCompletionInfo();
        }

        public void SequentialRun()
        {            
            mapperWatch.Start();
            while (true)
            {
                InputTextCunk input_chunk;
                int char_count = 0;

                char_count = reader.ReadChunk(out input_chunk, maxChunkSize);
                if (char_count == 0)
                    break;
                
                logger.Debug("Read a chunk: {0} records and {1} chars. InputQ count is {2}", StringFormatter.DigitGrouped(input_chunk.CharCount), StringFormatter.HumanReadablePostfixs(char_count), inputQ.Count);
                var dics=doMap(input_chunk);
                foreach (var dic in dics)
                {
                    doCombine(dic);
                    doSpillIfNeeded();
                }                
            }
            doSpillIfNeeded(true);
            mapperWatch.Stop();
            logCompletionInfo();
        }
    }
}
