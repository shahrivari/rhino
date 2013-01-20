using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Rhino;
using Rhino.IO;
using Rhino.IO.Records;
using Rhino.MapRed;
using System.Text.RegularExpressions;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            Serialization.RegisterBasicTypes();
            //var stream = new FileStream(@"Z:\pashm\130024591953953781-00000000-0000-0000-0000-000000000000-2", FileMode.Open);
            //while (stream.Position < stream.Length)
            //{
            //    var key=IntermediateRecord<string, int>.ReadKey(stream);
            //    var val_len = IntermediateRecord<string, int>.ReadValueListLength(stream);
            //    stream.Seek(val_len, SeekOrigin.Current);
            //}
            //"Z:\pashm\130024608013884320-27620cbf-350c-4b10-8c1c-04a040b903ab-1"

            //var merger = new IntermediateFileMerger<string, int>(@"z:\pashm", new Guid("bf037859-9cdc-44d3-8bf0-43c03021bac9"));            
            //var last=merger.Merge();
            //Console.WriteLine(last);

            //var stream = new FileStream(@"Z:\pashm\130031501103930168-02d379b7-01a7-4403-9bd9-d95a15099dbf-1", FileMode.Open);
            ////var stream = new FileStream(last, FileMode.Open);
            //var len = stream.Length;
            //while (true)
            //{
            //    KeyValuePair<string, List<int>> pair;
            //    var last2 = IntermediateRecord<string, int>.ReadRecord(stream, out pair);
            //    var pos = stream.Position;
            //    Console.WriteLine("{0}:{1}", pair.Key, pair.Value.Count);
            //}


            //var reader = new ReduceInputReader<string, int>(@"Z:\pashm\130031445985027547-00000000-0000-0000-0000-000000000000-7");
            //while (true)
            //{
            //    var ro = reader.GetNextReduceObject();
            //    var key = ro.Key;
            //    long count = 0;
            //    while (ro.HasMoreItems)
            //    {
            //        var next = ro.Next();
            //        count += next;
            //    }
            //}


            var reader = new RandomIntegerReader(1000*1000*5);
            
            var mr = new TextMapReduce<int, Int16>(reader, @"z:\pashm");
            mr.MapFunc = (s, context) =>
            {
                int x = Int32.Parse(s);
                context.Emit(x, 1);
            };

            //mr.CombineFunc = (list) => { return list.Sum(); };
            mr.ReduceFunc = (reduce_obj, reduce_context) =>
            {
                long sum = 0;
                while (reduce_obj.HasMoreItems)
                    sum += reduce_obj.Next();
                reduce_context.Emit(reduce_obj.Key + " : " + sum);
            };

            mr.Run();

            //var reader = new TextFileInputReader(@"z:\large.txt");
            //var mr=new TextMapReduce<string,int>(reader,@"z:\pashm");
            //mr.MapFunc=(s, context) => 
            //    {
            //        string s1 = Regex.Replace(s, "[^a-zA-Z0-9 -]", "");
            //        foreach (var token in s1.Split())
            //        {
            //            if (token.Length > 2)
            //                context.Emit(token.ToLower(), 1);
            //        }
            //        //if(s.Contains("yes"))
            //        //    context.Emit("length", s.Length);
            //        //for (int i = 0; i < 10000; i++);
            //    };
 
            //mr.CombineFunc=(list) => { return list.Sum(); };
            //mr.ReduceFunc = (reduce_obj, reduce_context) =>
            //{
            //    long sum = 0;
            //    while (reduce_obj.HasMoreItems)
            //        sum += reduce_obj.Next();
            //    reduce_context.Emit(reduce_obj.Key + " : " + sum.ToString());
            //};

            //mr.Run(1);

            //TextFileInputReader reader = new TextFileInputReader(@"z:\large.txt");
            //var mapper = new TextMapper<string, int>(reader, 
            //    (s, context) => 
            //    {
            //        foreach (var token in s.Split())
            //            context.Emit(token, 1);
            //        //context.Emit("length", s.Length);
            //        //for (int i = 0; i < 20000; i++);
            //    } 
            //    ,(list) => { return list.Sum(); });
            //mapper.SequentialRun();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);
        }            
    }
}
