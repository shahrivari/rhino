using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Test
{
    class Alaki
    {
        private static List<string> input = new List<string>();
        
        private static void exec(int threadcount)
        {
            ParallelOptions options = new ParallelOptions();
            options.MaxDegreeOfParallelism = threadcount;
            Parallel.ForEach(Partitioner.Create(0, input.Count),options, (range) =>
            {
                var dic = new Dictionary<string, List<int>>();
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    //make some delay!
                    //for (int x = 0; x < 400000; x++) ;                    
                    
                    var tokens = input[i].Split();
                    foreach (var token in tokens)
                    {
                        if (!dic.ContainsKey(token))
                            dic[token] = new List<int>();
                        dic[token].Add(1);
                    }
                }
            });

        }
        
        public static void Main(String[] args)
        {            
            StreamReader reader=new StreamReader((@"c:\txt-set\agg.txt"));
            while(true)
            {
                var line=reader.ReadLine();
                if(line==null)
                    break;
                input.Add(line);
            }

            for (int i = 0; i < 3; i++)
            {
                var input2 = new List<string>(input);
                input2.AddRange(input);
                input = input2;
            }

            

            
            DateTime t0 = DateTime.Now;
            //exec(Environment.ProcessorCount);
            Console.WriteLine("Parallel:  " + (DateTime.Now - t0));
            t0 = DateTime.Now;
            exec(1);
            Console.WriteLine("Serial:  " + (DateTime.Now - t0));
        }
    }

    
}
