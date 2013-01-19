using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino.IO;


namespace Rhino.MapRed
{
    public class TextReducer <InterKey,InterVal>
    {
        string inputPath;
        StreamWriter outputStream;
        int bufferSize = 32 * 1024 * 1024;
        string outputDirectory;
        Guid reducerID;
        public Guid ReducerID
        {
            get { return reducerID; }
        }

        private Action<InterKey, ReduceIterator<InterKey, InterVal> , ReduceContext> reduceFunc = null;
        private Action<InterKey, ReduceIterator<InterKey, InterVal>, ReduceContext> ReduceFunc
        {
            get { return reduceFunc; }
        }


        public TextReducer(string input_path, string output_dir, Guid id=default(Guid))
        {
            inputPath=input_path;
            outputDirectory=output_dir;
            reducerID=id;
            string out_file_name = DateTime.Now.ToFileTime().ToString() + "-" + reducerID;
            outputStream = new StreamWriter(out_file_name, false, Encoding.UTF8, bufferSize);
        }

        public void Reduce()
        {
            var reader = new ReduceInputReader<InterKey, InterVal>(inputPath, bufferSize);

        }
    }
}
