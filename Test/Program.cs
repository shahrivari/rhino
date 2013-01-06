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
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            TextFileInputReader reader = new TextFileInputReader(@"X:\alaki\BIN_NORM_20_100M.csv");
            TextMapper<int, int> mapper = new TextMapper<int, int>(reader, (s, context) => { for (int i = 0; i < 1000; i++); });
            mapper.Run();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);
            Console.ReadLine();
        }            
    }
}
