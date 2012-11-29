using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public abstract class SerialMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        private List<Tuple<InKey, InValue>> input;
        public List<Tuple<InKey, InValue>> Input
        {
            get { return input; }
            set 
            { 
                input = value;
                exlusiveEnd = value.Count;
            }
        }

        int inclusiveStart = 0;
        public int InclusiveStart
        {
            get { return inclusiveStart; }
            set { inclusiveStart = value; }
        }


        int exlusiveEnd = 0;
        public int ExlusiveEnd
        {
            get { return exlusiveEnd; }
            set { exlusiveEnd = value; }
        }


        Dictionary<InterKey,List<InterValue>> intermediateStore=new Dictionary<InterKey,List<InterValue>>();

        List<KeyValuePair<OutKey, OutValue>> outputStore = new List<KeyValuePair<OutKey, OutValue>>();
        public List<KeyValuePair<OutKey, OutValue>> OutputStore
        {
            get { return outputStore; }
        }
        
        public void Run()
        {
            foreach (var tuple in input)
            {
                var result=map(tuple.Item1, tuple.Item2);
                foreach (var r in result)
                {
                    if (!intermediateStore.ContainsKey(r.Key))
                        intermediateStore.Add(r.Key, new List<InterValue>());
                    intermediateStore[r.Key].Add(r.Value);
                }
            }

            //foreach (var pair in intermediateStore)
            //{
            //    var result = reduce(pair.Key, pair.Value);
            //    outputStore.Add(new KeyValuePair<OutKey, OutValue>(result.Key, result.Value));
            //}
        }

    }
}
