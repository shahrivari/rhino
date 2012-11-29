using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino;

namespace Test
{
    class Program
    {
        class MapRed : SortingMapReduce<string,byte,string,int,string,int>
        {
            protected override IEnumerable<KeyValuePair<string,int>>  map(string key, byte value)
            {
                List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();
                foreach (var token in key.Split())
                    result.Add(new KeyValuePair<string, int>(token.ToUpper(), 1));
                return result;
            }

            protected override KeyValuePair<string, int> reduce(string key, IEnumerable<int> values)
            {
                return new KeyValuePair<string, int>(key, values.Count());
                //for (int i = 0; i < 100000; i++) ;
            }
        }

        class MapRed2 : SerialMapReduce<string, byte, string, int, string, int>
        {
            protected override IEnumerable<KeyValuePair<string, int>> map(string key, byte value)
            {
                List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();
                foreach (var token in key.Split())
                    result.Add(new KeyValuePair<string, int>(token.ToUpper(), 1));
                //for (int i = 0; i < 100000; i++) ;
                return result;
            }

            protected override KeyValuePair<string, int> reduce(string key, IEnumerable<int> values)
            {
                return new KeyValuePair<string, int>(key, values.Count());
                //for (int i = 0; i < 100000; i++) ;
            }
        }


        static void Main(string[] args)
        {
            DateTime date = DateTime.Now;
            var m = new MapRed();
            var input = new List<Tuple<string, byte>>();
            StreamReader reader = new StreamReader(@"C:\downloads\big.txt");
            while (reader.Peek() != -1)
            {
                var line = reader.ReadLine();
                for(int i=0;i<1;i++)
                input.Add(new Tuple<string, byte>(line, 0));
            }
            m.Input = input;

            var elapsed = DateTime.Now - date;
            Console.WriteLine("Elapsed: {0}", elapsed);
            
            m.Run();

            elapsed = DateTime.Now - date;
            Console.WriteLine("Elapsed: {0}", elapsed);
            date = DateTime.Now;

            var m2 = new MapRed2();
            m2.Input = input;
            m2.Run();

            elapsed = DateTime.Now - date;
            Console.WriteLine("Elapsed: {0}", elapsed);




            //var result = from x in m.OutputStore orderby x.Item2 descending select x; 
            //Console.ReadKey();
        }
    }
}
