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
                        
            ////var stream = new FileStream(@"z:\pashm\130027304404358418-bf037859-9cdc-44d3-8bf0-43c03021bac9-5", FileMode.Open);
            //var stream = new FileStream(last, FileMode.Open);
            //var len = stream.Length;
            //while (true)
            //{
            //    KeyValuePair<string, List<int>> pair;
            //    var last2 = IntermediateRecord<string, int>.ReadRecord(stream, out pair);
            //    var pos = stream.Position;
            //}

                                    
            //TextFileInputReader reader = new TextFileInputReader(@"X:\alaki\BIN_NORM_20_40M.csv");

            //watch.Restart();
            //FileStream stre = new FileStream(@"z:\large1G.txt", FileMode.Open);
            //var len = stre.Length;
            //var buffer=new byte[1024*1024];
            //var q = new Queue<byte>();
            //var token = new StringBuilder();
            //for (int i = 0; i < len;)
            //{
            //    int read = stre.Read(buffer, 0, buffer.Length);
            //    i += read;
            //    for (int x = 0; x < read; x++)
            //    {
            //        char c = (char)buffer[x];
            //        if (c == '\n')
            //        {
            //            var xx = token.ToString(); ;
            //            token.Clear();
            //            continue;
            //        }
            //        token.Append(c);
            //    }
            //}
            //watch.Stop();
            //Console.WriteLine(watch.Elapsed);
            //watch.Restart();
            //stre.Close();

            var mr=new TextMapReduce<string,int>(@"z:\large.txt",@"z:\pashm");
            mr.MapFunc=(s, context) => 
                {
                    foreach (var token in s.Split())
                    {
                        if (token.Length > 2)
                            context.Emit(token, 1);
                    }
                    //if(s.Contains("yes"))
                    //    context.Emit("length", s.Length);
                    //for (int i = 0; i < 10000; i++);
                };
 
            //mr.CombineFunc=(list) => { return list.Sum(); };
            mr.Run(1);

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
