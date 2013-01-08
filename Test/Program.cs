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
            var serializer = Serializer.GetBinarySerializer(typeof(int));            
            var record1 = new SmallRecord(serializer.Invoke(100));
            var record2 = new SmallRecord(serializer.Invoke(256));

            int y = (int)Serializer.GetBinaryDeserializer(typeof(int)).Invoke(record1.Bytes, 4);

            int x = record1.CompareTo(record2);

            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            TextFileInputReader reader = new TextFileInputReader(@"C:\big.txt");
            var mapper = new TextMapper<string, int>(reader, 
                (s, context) => 
                {
                    //foreach (var token in s.Split())
                      //  context.Emit(token, 1);
                    context.Emit("length", s.Length);
                    /*for (int i = 0; i < 1000; i++);*/ 
                } 
                ,(list) => { return list.Sum(); });
            mapper.Run();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);
        }            
    }
}
