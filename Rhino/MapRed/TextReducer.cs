using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino.IO;
using Rhino.Util;
using NLog;


namespace Rhino.MapRed
{
    public class TextReducer <InterKey,InterVal>
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        string inputPath;
        StreamWriter outputStream;
        int bufferSize = 32 * 1024 * 1024;
        string outputDirectory;
        string outputFileName;

        Guid reducerID;
        public Guid ReducerID
        {
            get { return reducerID; }
        }

        private Action<ReduceObject<InterKey, InterVal> , ReduceContext> reduceFunc = null;
        public Action<ReduceObject<InterKey, InterVal>, ReduceContext> ReduceFunc
        {
            get { return reduceFunc; }
            set { reduceFunc = value; }
        }


        public TextReducer(string input_path, string output_dir, Guid id=default(Guid))
        {
            inputPath=input_path;
            outputDirectory=output_dir;
            reducerID=id;
            outputFileName = DateTime.Now.ToFileTime().ToString() + "-" + reducerID;
            outputStream = new StreamWriter(output_dir+"/"+outputFileName, false, Encoding.UTF8, bufferSize);
        }

        public void Reduce()
        {
            if (reduceFunc == null)
                throw new InvalidOperationException("Reduce function is not defined!");

            logger.Info("Reducing final file: {0}", inputPath);

            var reader = new ReduceInputReader<InterKey, InterVal>(inputPath, bufferSize);
            var reduce_context=new ReduceContext(outputStream);
            while (!reader.IsFinished)
            {
                var reduce_object=reader.GetNextReduceObject();
                reduceFunc.Invoke(reduce_object, reduce_context);
            }
            logger.Info("Reducer emitted {0} records.", StringFormatter.DigitGrouped(reduce_context.EmitCount));
            logger.Info("Reducer output: {0}.", outputFileName);
            outputStream.Close();
        }
    }
}
