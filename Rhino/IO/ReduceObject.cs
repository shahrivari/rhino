using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.IO
{
    public class ReduceObject<InterKey, InterVal>
    {
        InterKey key;
        public InterKey Key
        {
            get { return key; }
        }

        bool isFinished = false;
        List<InterVal> vals;
        int pos = 0;

        ReduceInputReader<InterKey, InterVal> reader; 

        public bool HasMoreItems
        {
            get
            {
                if (isFinished && pos >= vals.Count)
                    return false;
                else
                    return true;
            }
        }

        public ReduceObject(InterKey key, List<InterVal> initial_list, ReduceInputReader<InterKey,InterVal> reader , byte last_chunk=0)
        {
            this.key = key;
            addChunk(initial_list, last_chunk);
            this.reader = reader;
        }

        private void addChunk(List<InterVal> initial_list, byte last_chunk = 0)
        {
            if (initial_list == null)
                throw new InvalidOperationException("Initial list must not be null.");
            vals = initial_list;
            pos = 0;
            isFinished = last_chunk==0;
        }

        public InterVal Next()
        {
            if (isFinished)
            {
                if (pos >= vals.Count)
                    throw new InvalidOperationException("The iterator is expired!");
                else
                    return vals[pos++];
            }
            else
            {
                if(pos<vals.Count)
                    return vals[pos++];
                else
                {
                    var last=reader.ReadNextChunk(key, out vals);
                    pos = 0;
                    isFinished = last == 0;
                    return vals[pos++];
                }
            }
        }
        
    }
}
