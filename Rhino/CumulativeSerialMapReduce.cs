using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public abstract class CumulativeSerialMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
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


        List<KeyValuePair<InterKey, InterValue>> intermediateStore = new List<KeyValuePair<InterKey, InterValue>>();
        
        List<Tuple<OutKey, OutValue>> outputStore = new List<Tuple<OutKey, OutValue>>();
        public List<Tuple<OutKey, OutValue>> OutputStore
        {
            get { return outputStore; }
        }

        public void Run()
        {
            foreach (var tuple in input)
            {
                var result = map(tuple.Item1, tuple.Item2);
                foreach (var r in result)
                    intermediateStore.Add(new KeyValuePair<InterKey, InterValue>(r.Key, r.Value));
            }

            //intermediateStore.Sort();

            var x = from t in intermediateStore orderby t.Key select t;
            var m = x.Last();
                        
            //foreach (var pair in intermediateStore)
            //{
            //    var result = reduce(pair.Key, pair.Value);
            //    outputStore.Add(new Tuple<OutKey, OutValue>(result.Key, result.Value));
            //}
        }

    }
}
