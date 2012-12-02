using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Rhino;

namespace Test
{
    class Program
    {
        static void Main1(string[] args)
        {
            

            List<KeyValuePair<string, string>> input = new List<KeyValuePair<string, string>>();

            for (int i = 0; i < 1; i++)
            {
                LineReader reader = new LineReader(File.ReadAllText(@"c:\txt-set\pg11.txt"));
                while (reader.HasNextRecord())
                    input.Add(reader.readNextRecord());
            }

            DateTime t0 = DateTime.Now;

            Action<string,string,IMapReduceContext<string,int>> map = (mk, mv,context) => 
                {
                    var tokens = mk.Split();
                    foreach (var token in tokens)
                        context.Emit(token, 1);
                };

            Action<string, IEnumerable<int>, IMapReduceContext<string, int>> reduce = (rk, rv, context) =>
                {
                    context.Emit(rk, rv.Count());
                };
            
            
            ConcurrentMapReduce<string, string, string, int, string, int> smr = new ConcurrentMapReduce<string, string, string, int, string, int>(
            //InMemoryMapReduce<string, string, string, int, string, int> smr = new InMemoryMapReduce<string, string, string, int, string, int>(
                map,reduce,input
                );
            
            smr.Run();

            Console.WriteLine("DONE:  "+(DateTime.Now-t0));
            //Console.ReadLine();
        }



        //class MapRed : SortingMapReduce<string,byte,string,int,string,int>
        //{
        //    protected override IEnumerable<KeyValuePair<string,int>>  map(string key, byte value)
        //    {
        //        List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();
        //        foreach (var token in key.Split())
        //            result.Add(new KeyValuePair<string, int>(token.ToUpper(), 1));
        //        return result;
        //    }

        //    protected override KeyValuePair<string, int> reduce(string key, IEnumerable<int> values)
        //    {
        //        return new KeyValuePair<string, int>(key, values.Count());
        //        //for (int i = 0; i < 100000; i++) ;
        //    }
        //}

        //class MapRed2 : SerialMapReduce<string, byte, string, int, string, int>
        //{
        //    protected override IEnumerable<KeyValuePair<string, int>> map(string key, byte value)
        //    {
        //        List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();
        //        foreach (var token in key.Split())
        //            result.Add(new KeyValuePair<string, int>(token.ToUpper(), 1));
        //        //for (int i = 0; i < 100000; i++) ;
        //        return result;
        //    }

        //    protected override KeyValuePair<string, int> reduce(string key, IEnumerable<int> values)
        //    {
        //        return new KeyValuePair<string, int>(key, values.Count());
        //        //for (int i = 0; i < 100000; i++) ;
        //    }
        //}

        //struct Pashm<K,V> {
        //    K key;
        //    V val;

        //    public Pashm(K k, V v) {
        //        key = k;
        //        val = v;
        //    }
        //}

        //static void Main(string[] args)
        //{
        //    DateTime date = DateTime.Now;
        //    //Alaki.xx();
        //    var count = 1 * 1000 * 1000;
        //    var ar=new Pashm<Object,Object>[count];
        //    for (int i = 0; i < count; i++)
        //        ar[i] = new Pashm<object, object>(new Object(), new Object());

        //    Console.WriteLine("used mem: " + GC.GetTotalMemory(true));

        //    GC.Collect();
        //    Console.WriteLine("DONE");
        //    Console.ReadLine();
        //    return;
            
            
            
        //    var m = new MapRed();
        //    var input = new List<Tuple<string, byte>>();
        //    StreamReader reader = new StreamReader(@"C:\downloads\big.txt");
        //    while (reader.Peek() != -1)
        //    {
        //        var line = reader.ReadLine();
        //        for(int i=0;i<1;i++)
        //        input.Add(new Tuple<string, byte>(line, 0));
        //    }
        //    m.Input = input;

        //    var elapsed = DateTime.Now - date;
        //    Console.WriteLine("Elapsed: {0}", elapsed);
            
        //    m.Run();

        //    elapsed = DateTime.Now - date;
        //    Console.WriteLine("Elapsed: {0}", elapsed);
        //    date = DateTime.Now;

        //    var m2 = new MapRed2();
        //    m2.Input = input;
        //    m2.Run();

        //    elapsed = DateTime.Now - date;
        //    Console.WriteLine("Elapsed: {0}", elapsed);




        //    //var result = from x in m.OutputStore orderby x.Item2 descending select x; 
        //    //Console.ReadKey();
        //}
    }
}
