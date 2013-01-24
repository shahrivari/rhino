using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.IO.Pesudo;
using Rhino.MapRed;

namespace PiTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            var count = 200000;
            var each = 10000;
            var reader = new IntegerSequenceGenerator(0, count);

            var mr = new TextMapReduce<string, long>(reader, @"z:\pashm");
            mr.MapFunc = (s, context) =>
            {
                var rand = new Random();
                long num = 0;
                for (int i = 0; i < each; i++)
                {
                    var x = rand.NextDouble();
                    var y = rand.NextDouble();
                    if(x*x+y*y<1)
                        num++;
                }
                context.Emit("pi", num);
            };

            mr.CombineFunc = (list) => { return list.Sum(); };
            mr.ReduceFunc = (reduce_obj, reduce_context) =>
            {
                var all = reduce_obj.ReadAll();
                var pi = 4 * all.First() / (double)(count * each);
                reduce_context.Emit(reduce_obj.Key + ":" + pi);
            };

            mr.Run();

            watch.Stop();
            Console.WriteLine("Took: " + watch.Elapsed);
        }
    }
}
