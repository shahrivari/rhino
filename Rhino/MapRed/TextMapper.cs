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
    /// <summary>
    /// Maps string records and emits some keys and values.
    /// </summary>
    /// <typeparam name="InterKey">type of the keys to be emitted</typeparam>
    /// <typeparam name="InterValue">type of the values to be emitted</typeparam>
    class TextMapper<InterKey, InterValue>
    {
        private Action<string, MapContext<InterKey, InterValue>> mapFunc = null;
        /// <summary>
        /// Gets or sets the map function.
        /// </summary>
        public Action<string, MapContext<InterKey, InterValue>> MapFunc
        {
            get { return mapFunc; }
            set { mapFunc = value; }
        }

        private Func<List<InterValue>, InterValue> combineFunc = null;
        /// <summary>
        /// Gets or sets the combiner function.
        /// </summary>
        public Func<List<InterValue>, InterValue> CombineFunc
        {
            get { return combineFunc; }
            set { combineFunc = value; }
        }

        protected TextInputReader reader;
        private Object diskLock=new object();
        
        private Guid mapperID;
        /// <summary>
        /// Gets the ID of mapper.
        /// </summary>
        public Guid MapperID
        {
            get { return mapperID; }
        }

        private string workingDirectory = @"z:\pashm";
        
        protected int maxChunkSize = 64 * 1024;
        TimeSpan minWorkPeriod = new TimeSpan(0, 0, 0, 0, 100);
        TimeSpan maxWorkPeriod = new TimeSpan(0, 0, 0, 0, 250);
        int maxCharsToMap = 32 * 1024 * 1024;
        InMemoryCombineStore<InterKey, InterValue> combineStore ;


        private static Logger logger = LogManager.GetCurrentClassLogger();
        
        TextMapperInfo mapperInfo = new TextMapperInfo();
        /// <summary>
        /// Gets the mapper information object.
        /// </summary>
        public TextMapperInfo MapperInfo
        {
            get { return mapperInfo; }
        }

        BlockingCollection<Dictionary<InterKey, List<InterValue>>> dicsQ = new BlockingCollection<Dictionary<InterKey, List<InterValue>>>(100);
        BlockingCollection<InputTextCunk> inputQ = new BlockingCollection<InputTextCunk>(4);

        /// <summary>
        /// the constructor
        /// </summary>
        /// <param name="reader">reader that reads input of the mapper</param>
        /// <param name="working_directory">the directory to store the intremediate files.</param>
        /// <param name="map_func">the function to be used as the map</param>
        /// <param name="combine_func">the function to be used for combining intermediate values</param>
        public TextMapper(TextInputReader reader, string working_directory, Action<string, MapContext<InterKey, InterValue>> map_func=null, Func<List<InterValue>, InterValue> combine_func = null)
        {
            this.reader = reader;
            mapFunc = map_func;
            combineFunc = combine_func;
            workingDirectory = working_directory;
            logger.Info("Mapper created.");
        }

        private void init()
        {
            mapperID = Guid.NewGuid();
            combineStore = new InMemoryCombineStore<InterKey, InterValue>(mapperID, mapperInfo, workingDirectory, combineFunc);
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


        private void combine()
        {            
            Stopwatch watch = new Stopwatch();

            while (!dicsQ.IsCompleted)
            {
                var dic = dicsQ.Take();
                //doCombine(dic);
                combineStore.Add(dic);
                combineStore.doSpillIfNeeded();
            }
            combineStore.doSpillIfNeeded(true);
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

        /// <summary>
        /// Run the mapper using a concurrent and asynchnorous manner
        /// </summary>
        /// <param name="thread_num">number of threads to use</param>
        public void Run(int thread_num=0)
        {
            init();
            mapperWatch.Restart();

            Thread input_reader_thread = new Thread(new ThreadStart(readInput));
            input_reader_thread.Start();
            Thread combiner_thread = new Thread(new ThreadStart(combine));
            combiner_thread.Start();            

            //readInput();
            consumeInput(thread_num);
            input_reader_thread.Join();
            combiner_thread.Join();
            mapperWatch.Stop();
            logCompletionInfo();
        }

        /// <summary>
        /// Run the mapper in a sequential flow. e.g. first read a chunk, map it and then add to combine-store.
        /// All of the process is done step bye step,. However, each step is done using parallelism facilities but steps are done in order.
        /// </summary>
        /// <param name="thread_num">number of threads to be used</param>
        public void SequentialRun(int thread_num = 0)
        {
            init();
            mapperWatch.Start();
            while (true)
            {
                InputTextCunk input_chunk;
                int char_count = 0;

                char_count = reader.ReadChunk(out input_chunk, maxChunkSize);
                if (char_count == 0)
                    break;
                
                logger.Info("File percentage consumed: {3}%.  Read a chunk: {0} records and {1} chars. InputQ count is {2}", StringFormatter.DigitGrouped(input_chunk.CharCount), StringFormatter.HumanReadablePostfixs(char_count), inputQ.Count,(100*reader.Position)/reader.Length);
                var dics=doMap(input_chunk,thread_num);
                foreach (var dic in dics)
                {
                    combineStore.Add(dic);
                    combineStore.doSpillIfNeeded(false,thread_num);
                }                
            }
            combineStore.doSpillIfNeeded(true,thread_num);
            mapperWatch.Stop();
            logCompletionInfo();
        }
    }
}
