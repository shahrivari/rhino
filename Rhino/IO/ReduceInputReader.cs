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

        InterKey currentKey;
        InterKey lastKey;


        public ReduceInputReader(string input_path, int buffer_size = 32 * 1024 * 1024)
        {
            inputStream = new FileStream(input_path, FileMode.Open, FileAccess.Read, FileShare.Read, buffer_size); 
        }


        public byte ReadNextChunk(object key, out List<InterVal> list)
        {
            if (!(key is InterKey))
                throw new InvalidCastException("Key has another type!!");
            
            InterKey _key = (InterKey)key;
            var read_key = IntermediateRecord<InterKey, InterVal>.ReadKey(inputStream);
            List<InterVal> vals;
            var last_chunk=IntermediateRecord<InterKey, InterVal>.ReadValueList(inputStream, out vals);
            list = vals;
            if (!read_key.Equals(_key))
                throw new InvalidOperationException("We have reached a different key!");
            
            return last_chunk;
        }

        public ReduceIterator<InterKey,InterVal> GetNextIterator()
        {
            var read_key = IntermediateRecord<InterKey, InterVal>.ReadKey(inputStream);
            List<InterVal> vals;
            var last_chunk = IntermediateRecord<InterKey, InterVal>.ReadValueList(inputStream, out vals);
            var iterator = new ReduceIterator<InterKey,InterVal>(read_key,vals,this,last_chunk);
            
            return iterator;
        }

        public InterKey GetNextKey()
        {
            var key=IntermediateRecord<InterKey, InterVal>.ReadKey(inputStream);

            return default(InterKey);
        }
    }
}
