using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Rhino.MapRed
{
    /// <summary>
    /// Represents a map context that allows to emmit key-value pairs.
    /// </summary>
    /// <typeparam name="EmitKey">type of the key to be emitted</typeparam>
    /// <typeparam name="EmitVal">type of the value to be emitted</typeparam>
    public class MapContext<EmitKey, EmitVal>
    {
        Dictionary<EmitKey, List<EmitVal>> dic;
        long emitCount = 0;
        
        /// <summary>
        /// Number of emmitions done yet.
        /// </summary>
        public long EmitCount
        {
            get { return emitCount; }
        }

        /// <summary>
        /// NUmber of distinct keys emitted.
        /// </summary>
        public long NewKeys
        {
            get { return dic.Keys.Count; }
        }

        /// <summary>
        /// the constructor.
        /// </summary>
        /// <param name="dic"></param>
        public MapContext(Dictionary<EmitKey, List<EmitVal>> dic)
        {
            this.dic = dic;
        }

        /// <summary>
        /// gathers key-value pair from a mapper.
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="val">the value</param>
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
