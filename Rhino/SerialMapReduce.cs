using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public class InMemoryMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {

        Dictionary<InterKey, List<InterValue>> intermediateDic = new Dictionary<InterKey, List<InterValue>>();
        List<KeyValuePair<OutKey, OutValue>> resultList = new List<KeyValuePair<OutKey, OutValue>>(1024);
        
        List<KeyValuePair<InKey, InValue>> input;

        public InMemoryMapReduce(Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc,
                               Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc
                               , List<KeyValuePair<InKey, InValue>> input)
        {
            this.mapFunc = mapFunc;
            this.reduceFunc = reduceFunc;
            this.input = input;          
        }

        public void Run()
        {
            var mapContext = new MapContext<InterKey, InterValue>(intermediateDic);
            var reduceContext = new ReduceContext<OutKey, OutValue>(resultList);

            foreach(var record in input)                
                mapFunc.Invoke(record.Key, record.Value, mapContext); 
            

            foreach (var pair in intermediateDic)
                reduceFunc.Invoke(pair.Key, pair.Value, reduceContext);
        }

    }
}
