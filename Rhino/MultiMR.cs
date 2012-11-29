using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using System.Text;

namespace Rhino
{
    public abstract class MultiMR<InKey, InValue, InterKey, InterValue, OutKey, OutValue> :
        MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        private int chunkSize = 1024;
        private List<Tuple<InKey, InValue>> input;
        public List<Tuple<InKey, InValue>> Input
        {
            get { return input; }
            set { input = value; }
        }


        public void Run()
        {
            var chunkSize = (input.Count / Environment.ProcessorCount*2)+1;
            List<Tuple<int,int>> ranges=new List<Tuple<int,int>>();
            for (int i = 0; i < input.Count; i++)
            {
                if(i+chunkSize>input.Count)
                    ranges.Add(new Tuple<int, int>(i, input.Count));
                else
                    ranges.Add(new Tuple<int, int>(i, i + chunkSize));

                i = i + chunkSize;
            }



            Dictionary<InterKey, List<InterValue>>[] cache = new Dictionary<InterKey, List<InterValue>>[ranges.Count];

            for (int i = 0; i < cache.Length; i++)
                cache[i] = new Dictionary<InterKey, List<InterValue>>();

            Parallel.For(0, ranges.Count, index =>
            {
                for (int i = ranges[index].Item1; i < ranges[index].Item2; i++)
                {
                    var xxx = map(input[i].Item1, input[i].Item2);
                    foreach (var r in xxx)
                    {
                        if (!cache[index].ContainsKey(r.Key))
                            cache[index].Add(r.Key, new List<InterValue>());
                        cache[index][r.Key].Add(r.Value);
                    }
                }
            }
            );


            //Dictionary<InterKey, List<InterValue>> overall = new Dictionary<InterKey, List<InterValue>>();
            //foreach (var t in cache)
            //{
            //    foreach (var pair in t)
            //    {
            //        if (!overall.ContainsKey(pair.Key))
            //            overall.Add(pair.Key, new List<InterValue>());
            //        overall[pair.Key].AddRange(pair.Value);
            //    }
            //}



        }


    }
}
