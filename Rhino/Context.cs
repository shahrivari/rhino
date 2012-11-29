using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    class Context<OutKey, OutValue>
    {
        public Context (Dictionary<OutKey, OutValue> table)
        {
            this.table = table;
        }
        
        public Dictionary<OutKey, OutValue> table;

        public void Write(OutKey key, OutValue value)
        {
        }
    }
}
