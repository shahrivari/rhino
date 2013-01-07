using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Rhino;
using Rhino.IO;
using Rhino.MapRed;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            float x = 1000;
            var serializer = Serializer.GetBinarySerializer(typeof(float));
            var bytes=serializer.Invoke(x);
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            TextFileInputReader reader = new TextFileInputReader(@"X:\alaki\BIN_NORM_20_10M.csv");
            var mapper = new TextMapper<string, int>(reader, 
                (s, context) => 
                {
                    foreach(var token in s.Split())
                        context.Emit(token, 1);
                    /*for (int i = 0; i < 1000; i++);*/ 
                } 
                ,(list) => { return list.Sum(); });
            mapper.Run();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);
            Console.ReadLine();
        }            
    }
}
