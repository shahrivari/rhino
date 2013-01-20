using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino.IO.Records;

namespace Rhino.IO
{
    public class ReduceInputReader <InterKey, InterVal>
    {
        //public delegate List<InterVal> ReadNext(object key);

        Stream inputStream;
        long inputLength;

        public bool IsFinished
        {
            get 
            {
                return inputStream.Position >= inputLength;
            }
        }

        public ReduceInputReader(string input_path, int buffer_size = 32 * 1024 * 1024)
        {
            inputStream = new FileStream(input_path, FileMode.Open, FileAccess.Read, FileShare.Read, buffer_size);
            inputLength = inputStream.Length;
        }


        public byte ReadNextChunk(InterKey key, out List<InterVal> list)
        {
            InterKey _key = (InterKey)key;
            var read_key = IntermediateRecord<InterKey, InterVal>.ReadKey(inputStream);
            List<InterVal> vals;
            var last_chunk=IntermediateRecord<InterKey, InterVal>.ReadValueList(inputStream, out vals);
            list = vals;
            if (!read_key.Equals(_key))
                throw new InvalidOperationException("We have reached a different key!");
            
            return last_chunk;
        }

        public ReduceObject<InterKey,InterVal> GetNextReduceObject()
        {
            var read_key = IntermediateRecord<InterKey, InterVal>.ReadKey(inputStream);
            List<InterVal> vals;
            var last_chunk = IntermediateRecord<InterKey, InterVal>.ReadValueList(inputStream, out vals);
            var iterator = new ReduceObject<InterKey,InterVal>(read_key,vals,this,last_chunk);
            
            return iterator;
        }

        public InterKey GetNextKey()
        {
            var key=IntermediateRecord<InterKey, InterVal>.ReadKey(inputStream);

            return default(InterKey);
        }
    }
}
