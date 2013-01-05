using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Rhino.MapRed
{
    public class MapContext<EmitKey, EmitVal>
    {
        Dictionary<EmitKey, List<EmitVal>> dic;
        long emitCount = 0;
        public long EmitCount
        {
            get { return emitCount; }
        }

        public long NewKeys
        {
            get { return dic.Keys.Count; }
        }

        public MapContext(Dictionary<EmitKey, List<EmitVal>> dic)
        {
            this.dic = dic;
        }

        public void Emit(EmitKey key, EmitVal val)
        {
            List<EmitVal> list;
            Interlocked.Increment(ref emitCount);
            var found = dic.TryGetValue(key, out list);
            if (found)
                list.Add(val);
            else
            {
                list = new List<EmitVal>();
                list.Add(val);
                dic[key] = list;
            }
        }
    }
}
