using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Text;

namespace Rhino
{
    public class ConcurrentMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        object lockObject=new object();
        List<Dictionary<InterKey, List<InterValue>>> dics = new List<Dictionary<InterKey, List<InterValue>>>();
        List<KeyValuePair<InKey, InValue>> input;

        public ConcurrentMapReduce(Action<InKey, InValue, IMapReduceContext<InterKey, InterValue>> mapFunc,
                               Action<InterKey, IEnumerable<InterValue>, IMapReduceContext<OutKey, OutValue>> reduceFunc
                               , List<KeyValuePair<InKey, InValue>> input)
        {
            this.mapFunc = mapFunc;
            this.reduceFunc = reduceFunc;
            this.input = input;          
        }

      

        public void Run()
        {
            Parallel.ForEach(Partitioner.Create(0, input.Count), (range) =>
            {
                Console.WriteLine(range);
                var dic = new Dictionary<InterKey, List<InterValue>>();
                var context = new MapContext<InterKey, InterValue>(dic);
                for(int i=range.Item1;i<range.Item2;i++)
                    mapFunc.Invoke(input[i].Key, input[i].Value, context);
                //lock (lockObject)
                //{
                //    dics.Add(dic);
                //}
            });

        }




    }
}
