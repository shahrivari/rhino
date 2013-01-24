using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NLog;
using System.Diagnostics;
using Rhino.IO;
using Rhino.IO.Records;

namespace Rhino.MapRed
{
    /// <summary>
    /// Represents a MapReduce Job that consumes text records and produces text records as the output.
    /// </summary>
    /// <typeparam name="InterKey">type of intermediate key</typeparam>
    /// <typeparam name="InterValue">type of intermediate values</typeparam>
    public class TextMapReduce<InterKey, InterValue>
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        TextMapper<InterKey, InterValue> mapper;
        IntermediateFileMerger<InterKey, InterValue> merger;
        TextReducer<InterKey, InterValue> reducer;
        TextInputReader inputReader;

        Action<string, MapContext<InterKey, InterValue>> mapFunc;
        /// <summary>
        /// The map function for mapping the input
        /// </summary>
        public Action<string, MapContext<InterKey, InterValue>> MapFunc
        {
            set { mapFunc = value; }
        }

        Func<List<InterValue>, InterValue> combineFunc;
        /// <summary>
        /// the combine function used for combining the intermediate values.
        /// </summary>
        public Func<List<InterValue>, InterValue> CombineFunc
        {
            set { combineFunc = value; }
        }

        Action<ReduceObject<InterKey, InterValue>, ReduceContext> reduceFunc;
        /// <summary>
        /// the reduce function used for reducing the intermediate key-value pairs.
        /// </summary>
        public Action<ReduceObject<InterKey,InterValue>, ReduceContext> ReduceFunc
        {
            set { reduceFunc = value; }
        }


        string workingDirectory;
        
        /// <summary>
        /// the constructor
        /// </summary>
        /// <param name="input_reader">the reader used for reading the input</param>
        /// <param name="working_dir">the working directory used for storing the intermediate files and currently the output</param>
        public TextMapReduce(TextInputReader input_reader, string working_dir)
        {
            workingDirectory = working_dir;
            inputReader = input_reader;
        }

        /// <summary>
        /// Runs the MapReduce Job
        /// </summary>
        /// <param name="thread_num">number of threads to use.</param>
        public void Run(int thread_num=0)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            mapper = new TextMapper<InterKey, InterValue>(inputReader, workingDirectory);
            mapper.MapFunc = mapFunc;
            mapper.CombineFunc = combineFunc;
            mapper.SequentialRun(thread_num);
            
            merger = new IntermediateFileMerger<InterKey, InterValue>(workingDirectory, mapper.MapperID);
            var last_file = merger.Merge();
            logger.Info("Merged to {0}", last_file);

            reducer = new TextReducer<InterKey, InterValue>(last_file, workingDirectory, mapper.MapperID);
            reducer.ReduceFunc = reduceFunc;
            reducer.Reduce();
            watch.Stop();
            logger.Info("Job took {0}.", watch.Elapsed);
        }


        
    }
}
