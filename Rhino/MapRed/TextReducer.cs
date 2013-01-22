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
    /// <summary>
    /// Represents the reducer class
    /// </summary>
    /// <typeparam name="InterKey">type of intermediate keys</typeparam>
    /// <typeparam name="InterVal">type of the intermediate values</typeparam>
    class TextReducer <InterKey,InterVal>
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        string inputPath;
        StreamWriter outputStream;
        int bufferSize = 32 * 1024 * 1024;
        string outputDirectory;
        string outputFileName;

        Guid reducerID;
        /// <summary>
        /// the ID of the reducer.
        /// </summary>
        public Guid ReducerID
        {
            get { return reducerID; }
        }

        private Action<ReduceObject<InterKey, InterVal> , ReduceContext> reduceFunc = null;
        /// <summary>
        /// the function used for reducing the intermediate data
        /// </summary>
        public Action<ReduceObject<InterKey, InterVal>, ReduceContext> ReduceFunc
        {
            get { return reduceFunc; }
            set { reduceFunc = value; }
        }

        /// <summary>
        /// the constructor
        /// </summary>
        /// <param name="input_path">path of the cumolative input intermedate file containing the merged intermediate data.</param>
        /// <param name="output_dir">the output directory for storing the output data</param>
        /// <param name="id">ID of the mapper</param>
        public TextReducer(string input_path, string output_dir, Guid id=default(Guid))
        {
            inputPath=input_path;
            outputDirectory=output_dir;
            reducerID=id;
            outputFileName = DateTime.Now.ToFileTime().ToString() + "-" + reducerID;
            outputStream = new StreamWriter(output_dir+"/"+outputFileName, false, Encoding.UTF8, bufferSize);
        }

        /// <summary>
        /// performs the reduce phase
        /// </summary>
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
