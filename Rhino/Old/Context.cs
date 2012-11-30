using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Old
{
    public class Context<OutKey, OutValue>
    {
        public Dictionary<OutKey, OutValue> table;

        public Context (Dictionary<OutKey, OutValue> table)
        {
            this.table = table;
        }      
        

        public void Emmit(OutKey key, OutValue value)
        {
        }
    }
}
