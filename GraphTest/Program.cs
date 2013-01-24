using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Rhino.IO;
using Rhino.MapRed;

namespace GraphTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            var reader = new TextFileLineByLineReader(@"Z:\ClueWeb09_WG_50m.graph-txt", 1);
            //reader.PrependLineNumber = true;
            reader.LinesPerRecord = 10000;

            var keyword = "11";

            var mr = new TextMapReduce<string, long>(reader, @"z:\pashm");
            mr.MapFunc = (s, context) =>
            {
                context.Emit("found", Regex.Matches(s, keyword).Count);
            };

            mr.CombineFunc = (list) => { return list.Sum(); };
            mr.ReduceFunc = (reduce_obj, reduce_context) =>
            {
                var all = reduce_obj.ReadAll();
                reduce_context.Emit(reduce_obj.Key + ":" + all.First().ToString());
            };

            mr.Run(1);

            watch.Stop();
            Console.WriteLine("Took: " + watch.Elapsed);
        }
    }
}
