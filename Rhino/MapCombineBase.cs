using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Rhino
{
    public abstract class MapCombineBase<InKey, InValue, InterKey, InterValue>
    {
        protected sealed class MapContext<Key, Value> : IMapContext<Key, Value>
        {
            Dictionary<Key, List<Value>> dic;
            long emitCount = 0;
            public long EmitCount
            {
                get { return emitCount; }
            }

            long newKeys = 0;
            public long NewKeys
            {
                get { return newKeys; }
            }

            public MapContext(Dictionary<Key, List<Value>> dic)
            {                
                this.dic = dic;
            }

            public void Emit(Key key, Value val)
            {
                List<Value> list;
                Interlocked.Increment(ref emitCount);
                var found = dic.TryGetValue(key, out list);
                if (found)
                    list.Add(val);
                else
                {
                    Interlocked.Increment(ref newKeys);
                    list = new List<Value>();
                    list.Add(val);
                    dic[key] = list;
                }

            }
        }

        //protected sealed class CombineContext<Key, Value> : IMapCombineContext<Key, Value>
        //{
        //    List<KeyValuePair<Key, Value>> list;

        //    public CombineContext(List<KeyValuePair<Key, Value>> list)
        //    {
        //        this.list = list;
        //    }

        //    public void Emit(Key key, Value val)
        //    {
        //        list.Add(new KeyValuePair<Key, Value>(key, val));
        //    }
        //}

        //void map(InKey key, InValue value, IMapReduceContext<InterKey, InterValue> context);
        //void reduce(InterKey key, IEnumerable<InterValue> values, IMapReduceContext<OutKey, OutValue> context);
        protected Action<InKey, InValue, IMapContext<InterKey, InterValue>> mapFunc;
        //protected Action<InterKey, IEnumerable<InterValue>, IMapCombineContext<OutKey, OutValue>> combineFunc; 

    }
}
