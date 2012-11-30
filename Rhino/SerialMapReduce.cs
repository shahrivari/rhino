using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public class SerialMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : IMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        sealed class MapContext<Key, Value> : IMapReduceContext<Key, Value>
        {            
            //List<KeyValuePair<Key, Value>> list;
            Dictionary<Key, List<Value>> dic;

            //public MapContext(List<KeyValuePair<Key, Value>> list) 
            public MapContext(Dictionary<Key, List<Value>> dic) 
            {
                this.dic = dic;
            }

            public void Emit(Key key, Value val) 
            {
                if (!dic.ContainsKey(key))
                    dic[key] = new List<Value>();
                dic[key].Add(val);
                //list.Add(new KeyValuePair<Key,Value>(key,val));                
            }
        }

        sealed class ReduceContext<Key, Value> : IMapReduceContext<Key, Value>
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


        //List<KeyValuePair<InterKey, InterValue>> intermediateList = new List<KeyValuePair<InterKey, InterValue>>(1024);
        Dictionary<InterKey, List<InterValue>> intermediateList = new Dictionary<InterKey, List<InterValue>>();
        List<KeyValuePair<OutKey, OutValue>> resultList = new List<KeyValuePair<OutKey, OutValue>>(1024);
        MapContext<InterKey, InterValue> mapContext;
        ReduceContext<OutKey, OutValue> reduceContext;

        Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc;
        Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc; 

        public void map(InKey key, InValue value, IMapReduceContext<InterKey, InterValue> context)
        {
            mapFunc.Invoke(key, value, mapContext); 
        }

        public void reduce(InterKey key, IEnumerable<InterValue> values, IMapReduceContext<OutKey, OutValue> context)
        {
            reduceFunc.Invoke(key, values, reduceContext);
        }

        InputRecordReader<InKey, InValue> input;

        public SerialMapReduce(Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc,
                               Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc
                               , InputRecordReader<InKey, InValue> input)
        {
            this.mapFunc = mapFunc;
            this.reduceFunc = reduceFunc;
            this.input = input;
            mapContext = new MapContext<InterKey, InterValue>(intermediateList);
            reduceContext = new ReduceContext<OutKey, OutValue>(resultList);
        }

        public void Run()
        {
            while (input.HasNextRecord())
            {
                var record = input.readNextRecord();
                map(record.Key, record.Value, mapContext);
            }

            //var sorted = from pair in intermediateList orderby pair.Key select pair;
            //sorted.Count();

            foreach (var item in intermediateList)
            {
                reduce(item.Key, item.Value, reduceContext);
            }
        }

    }
}
