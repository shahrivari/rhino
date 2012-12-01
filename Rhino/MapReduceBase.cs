using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public abstract class MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        protected sealed class MapContext<Key, Value> : IMapReduceContext<Key, Value>
        {
            Dictionary<Key, List<Value>> dic;

            public MapContext(Dictionary<Key, List<Value>> dic)
            {
                this.dic = dic;
            }

            public void Emit(Key key, Value val)
            {
                List<Value> list;
                var found = dic.TryGetValue(key, out list);

                if (found)
                    list.Add(val);
                else
                {
                    list = new List<Value>();
                    list.Add(val);
                    dic[key] = list;
                }

            }
        }

        protected sealed class ReduceContext<Key, Value> : IMapReduceContext<Key, Value>
        {
            List<KeyValuePair<Key, Value>> list;

            public ReduceContext(List<KeyValuePair<Key, Value>> list)
            {
                this.list = list;
            }

            public void Emit(Key key, Value val)
            {
                list.Add(new KeyValuePair<Key, Value>(key, val));
            }
        }

        //void map(InKey key, InValue value, IMapReduceContext<InterKey, InterValue> context);
        //void reduce(InterKey key, IEnumerable<InterValue> values, IMapReduceContext<OutKey, OutValue> context);
        protected Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc;
        protected Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc; 

    }
}
