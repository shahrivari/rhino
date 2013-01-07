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
            int x = 0;
            var bytes = Serializer.GetBinarySerializer(typeof(int)).Invoke(x);
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            TextFileInputReader reader = new TextFileInputReader(@"X:\alaki\BIN_NORM_20_10M.csv");
            TextMapper<int, int> mapper = new TextMapper<int, int>(reader, (s, context) => { context.Emit(1, s.Length);/*for (int i = 0; i < 1000; i++);*/ });
            mapper.Run();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);
            Console.ReadLine();
        }            
    }
}
