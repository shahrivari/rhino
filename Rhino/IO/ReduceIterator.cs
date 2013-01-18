using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.IO
{
    public class ReduceIterator<InterVal>
    {
        Stream stream;
        object key;
        bool isFinished = false;
        List<InterVal> vals;
        int pos = 0;

        
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

        public ReduceIterator(List<InterVal> initial_list, bool last_chunk=true)
        {
            AddChunk(initial_list, last_chunk);
        }

        public void AddChunk(List<InterVal> initial_list, bool last_chunk = true)
        {
            if (initial_list == null)
                throw new InvalidOperationException("Initial list must not be null.");
            vals = initial_list;
            pos = 0;
            isFinished = last_chunk;
        }

        public InterVal Next()
        {
            if (isFinished && pos >= vals.Count)
                throw new InvalidOperationException("The iterator is expired!");


            return default(InterVal);
        }
        
    }
}
