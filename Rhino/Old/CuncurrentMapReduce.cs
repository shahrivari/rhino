using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Linq;
using System.Text;

namespace Rhino.Old
{
    abstract public class CuncurrentMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        private IEnumerable<Tuple<InKey, InValue>> input;
        public IEnumerable<Tuple<InKey, InValue>> Input
        {
            get { return input; }
            set { input = value; }
        }

        private ConcurrentBag<Tuple<InterKey, InterValue>> intermediateStore = new ConcurrentBag<Tuple<InterKey, InterValue>>();
        private ConcurrentDictionary<InterKey, ConcurrentBag<InterValue>> combineStore = new ConcurrentDictionary<InterKey, ConcurrentBag<InterValue>>();
        private ConcurrentBag<Tuple<OutKey, OutValue>> outputStore = new ConcurrentBag<Tuple<OutKey, OutValue>>();

        protected void map(Tuple<InKey, InValue> tuple)
        {
            map(tuple.Item1, tuple.Item2);
        }

        protected void reduce(KeyValuePair<InterKey, ConcurrentBag<InterValue>> tuple)
        {
            reduce(tuple.Key, tuple.Value);
        }

        protected void emitInMap(InterKey key, InterValue value)
        {
            intermediateStore.Add(new Tuple<InterKey,InterValue>(key,value)); 
        }

        protected void emitInReduce(OutKey key, OutValue value)
        {
            outputStore.Add(new Tuple<OutKey,OutValue>(key,value));            
        }

        private void combine(Tuple<InterKey, InterValue> tuple)
        {
            var key = tuple.Item1;
            var value = tuple.Item2;
            combineStore.AddOrUpdate(key, new ConcurrentBag<InterValue> { value }, (key1, oldvalue) => { oldvalue.Add(value); return oldvalue; });
        }

        public void Run()
        {
            Parallel.ForEach(input, new Action<Tuple<InKey, InValue>>(map));
            Parallel.ForEach(intermediateStore,new Action<Tuple<InterKey,InterValue>>(combine));
            Parallel.ForEach(combineStore, new Action<KeyValuePair<InterKey, ConcurrentBag<InterValue>>>(reduce));
        }

    }
}
