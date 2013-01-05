using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Text;

namespace Rhino.Old
{
    public class ConcurrentMapCombiner<InKey, InValue, InterKey, InterValue> : MapCombineBase<InKey, InValue, InterKey, InterValue>
    {
        ConcurrentBag<Dictionary<InterKey, List<InterValue>>> dics;
        List<KeyValuePair<InKey, InValue>> input;
        long mapCalls = 0;
        public long MapCalls
        {
            get { return mapCalls; }
        }

        long keyCount = 0;
        public long KeyCount
        {
            get { return keyCount; }
        }

        public ConcurrentMapCombiner(Action<InKey, InValue, IMapContext<InterKey, InterValue>> mapFunc, List<KeyValuePair<InKey, InValue>> input)
        {
            this.mapFunc = mapFunc;
            this.input = input;          
        }



        public ConcurrentBag<Dictionary<InterKey, List<InterValue>>> Run(int thread_num = 0)
        {
            dics = null;
            dics = new ConcurrentBag<Dictionary<InterKey, List<InterValue>>>();

            ParallelOptions option = new ParallelOptions();
            if (thread_num == 0)
                option.MaxDegreeOfParallelism = Environment.ProcessorCount*2;
            else
                option.MaxDegreeOfParallelism = thread_num;
            Parallel.ForEach(Partitioner.Create(0, input.Count), option, (range) =>
            {
                var dic = new Dictionary<InterKey, List<InterValue>>();
                var context = new MapContext<InterKey, InterValue>(dic);
                for(int i=range.Item1;i<range.Item2;i++)
                    mapFunc.Invoke(input[i].Key, input[i].Value, context);
                dics.Add(dic);
                Interlocked.Add(ref mapCalls, context.EmitCount);
                Interlocked.Add(ref keyCount, context.NewKeys);
            });

            return dics;
            //var interdic = new Dictionary<InterKey, List<InterValue>>(1024);
            //var reduceContext = new CombineContext<OutKey, OutValue>(resultList);
            //foreach (var dic in dics)
            //{
            //    foreach (var pair in dic)
            //    {
            //        if (!interdic.ContainsKey(pair.Key))
            //            interdic[pair.Key] = new List<InterValue>();
            //        interdic[pair.Key].AddRange(pair.Value);
            //    }
            //}

            //foreach (var pair in interdic)
            //    combineFunc.Invoke(pair.Key, pair.Value, reduceContext);
        }

          


    }
}
