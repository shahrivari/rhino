using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public class SerialMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {

        Dictionary<InterKey, List<InterValue>> intermediateDic = new Dictionary<InterKey, List<InterValue>>();
        List<KeyValuePair<OutKey, OutValue>> resultList = new List<KeyValuePair<OutKey, OutValue>>(1024);
        
        InputRecordReader<InKey, InValue> input;

        public SerialMapReduce(Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc,
                               Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc
                               , InputRecordReader<InKey, InValue> input)
        {
            this.mapFunc = mapFunc;
            this.reduceFunc = reduceFunc;
            this.input = input;          
        }

        public void Run()
        {
            var mapContext = new MapContext<InterKey, InterValue>(intermediateDic);
            var reduceContext = new ReduceContext<OutKey, OutValue>(resultList);

            while (input.HasNextRecord())
            {
                var record = input.readNextRecord();
                mapFunc.Invoke(record.Key, record.Value, mapContext); 
            }

            foreach (var pair in intermediateDic)
                reduceFunc.Invoke(pair.Key, pair.Value, reduceContext);
        }

    }
}
