using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Lab
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch watch = new Stopwatch();            
            //int count = 1 * 1000 * 1000;
            foreach (var count in new int[] { 1000000, 2000000, 4000000, 8000000 })
            {
                watch.Restart();
                var num_list = new List<string>(count);

                Random rand = new Random(123);
                for (int i = 0; i < count; i++)
                    num_list.Add(rand.Next().ToString());
                watch.Stop();
                var to_sort = num_list.ToArray();
                Console.Write("Random: " + watch.Elapsed);

                watch.Restart();
                //var sorted = num_list.OrderBy(s => s).ToArray();
                Array.Sort(to_sort);

                watch.Stop();
                Console.WriteLine("\tSort: " + watch.Elapsed);
            }

        }
    }
}
