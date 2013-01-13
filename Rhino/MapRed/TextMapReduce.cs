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

        public Action<string, MapContext<InterKey, InterValue>> MapFunc
        {
            set { mapper.MapFunc = value; }
        }

        public Func<List<InterValue>, InterValue> CombineFunc
        {
            set { mapper.CombineFunc = value; }
        }


        string workingDirectory;
        public TextMapReduce(string input_file, string working_dir)
        {
            workingDirectory = working_dir;
            mapper = new TextMapper<InterKey, InterValue>(new TextFileInputReader(input_file),workingDirectory);
        }

        public void Run(int thread_num=0)
        {
            mapper.SequentialRun(thread_num);
            merger = new IntermediateFileMerger<InterKey, InterValue>(workingDirectory, mapper.MapperID);
            var last_file = merger.Merge();
            logger.Info("Merged to {0}", last_file);
        }


        
    }
}
