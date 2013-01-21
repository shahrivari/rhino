using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NLog;

using Rhino.IO;
using Rhino.IO.Records;

namespace Rhino.MapRed
{
    public class TextMapReduce<InterKey, InterValue>
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        TextMapper<InterKey, InterValue> mapper;
        IntermediateFileMerger<InterKey, InterValue> merger;
        TextReducer<InterKey, InterValue> reducer;
        TextInputReader inputReader;

        Action<string, MapContext<InterKey, InterValue>> mapFunc;
        public Action<string, MapContext<InterKey, InterValue>> MapFunc
        {
            set { mapFunc = value; }
        }

        Func<List<InterValue>, InterValue> combineFunc;
        public Func<List<InterValue>, InterValue> CombineFunc
        {
            set { combineFunc = value; }
        }

        Action<ReduceObject<InterKey, InterValue>, ReduceContext> reduceFunc;
        public Action<ReduceObject<InterKey,InterValue>, ReduceContext> ReduceFunc
        {
            set { reduceFunc = value; }
        }


        string workingDirectory;
        public TextMapReduce(TextInputReader input_reader, string working_dir)
        {
            workingDirectory = working_dir;
            inputReader = input_reader;
        }

        
        public void Run(int thread_num=0)
        {
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
        }


        
    }
}
