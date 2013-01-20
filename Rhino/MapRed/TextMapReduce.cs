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

        public Action<string, MapContext<InterKey, InterValue>> MapFunc
        {
            set { mapper.MapFunc = value; }
        }

        public Func<List<InterValue>, InterValue> CombineFunc
        {
            set { mapper.CombineFunc = value; }
        }

        public Action<ReduceObject<InterKey,InterValue>, ReduceContext> ReduceFunc
        {
            set { reducer.ReduceFunc = value; }
        }


        string workingDirectory;
        public TextMapReduce(TextInputReader input_reader, string working_dir)
        {
            workingDirectory = working_dir;
            mapper = new TextMapper<InterKey, InterValue>(input_reader,workingDirectory);
            reducer = new TextReducer<InterKey, InterValue>("", workingDirectory, mapper.MapperID);
        }

        public void Run(int thread_num=0)
        {
            mapper.SequentialRun(thread_num);
            merger = new IntermediateFileMerger<InterKey, InterValue>(workingDirectory, mapper.MapperID);
            var last_file = merger.Merge();
            logger.Info("Merged to {0}", last_file);
            var new_reducer = new TextReducer<InterKey, InterValue>(last_file, workingDirectory, mapper.MapperID);
            new_reducer.ReduceFunc = reducer.ReduceFunc;
            reducer = new_reducer;
            reducer.Reduce();            
        }


        
    }
}
