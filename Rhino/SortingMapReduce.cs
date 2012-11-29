using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;

namespace Rhino
{
    public abstract class SortingMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue> : 
        MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        private int chunkSize=10240;
        private List<Tuple<InKey, InValue>> input;
        public  List<Tuple<InKey, InValue>> Input
        {
            get { return input; }
            set { input = value; }
        }

        ConcurrentBag<KeyValuePair<OutKey, OutValue>> outputStore = new ConcurrentBag<KeyValuePair<OutKey, OutValue>>();
        public ConcurrentBag<KeyValuePair<OutKey, OutValue>> OutputStore
        {
            get { return outputStore; }
        }

        public void Run()
        {
            //var ranges=Partitioner.Create(0, input.Count, chunkSize);
            var ranges = Partitioner.Create(0,input.Count);
            ConcurrentBag<Dictionary<InterKey, List<InterValue>>> intermediateResults = new ConcurrentBag<Dictionary<InterKey, List<InterValue>>>();

            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Environment.ProcessorCount*2;
            Parallel.ForEach(ranges,options ,range =>
                {
                    Dictionary<InterKey, List<InterValue>> intermediateResult = new Dictionary<InterKey, List<InterValue>>();
                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        var xxx=map(input[i].Item1, input[i].Item2);
                        foreach (var r in xxx)
                        {
                            if(!intermediateResult.ContainsKey(r.Key))
                                intermediateResult.Add(r.Key,new List<InterValue>());
                            intermediateResult[r.Key].Add(r.Value);
                        }
                    }
                    intermediateResults.Add(intermediateResult);
                }
            );


            //Dictionary<InterKey, List<InterValue>> overall = new Dictionary<InterKey, List<InterValue>>();
            //foreach (var t in intermediateResults)
            //{
            //    foreach (var pair in t)
            //    {
            //        if (!overall.ContainsKey(pair.Key))
            //            overall.Add(pair.Key, new List<InterValue>());
            //        overall[pair.Key].AddRange(pair.Value);
            //    }
            //}


            ////var reduceranges = Partitioner.Create(overall);

            //object lockObj = new object();

            //Parallel.ForEach(overall, options, pair => 
            //{
            //    var result = reduce(pair.Key, pair.Value);
            //    outputStore.Add(new KeyValuePair<OutKey, OutValue>(result.Key, result.Value));
            //}
            //);
            
            
        }


    }
}
