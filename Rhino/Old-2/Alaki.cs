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
        
        
        public static void Main2(String[] args)
        {            
            DateTime t0 = DateTime.Now;
            Console.WriteLine("Parallel:  " + (DateTime.Now - t0));
            t0 = DateTime.Now;
            Console.WriteLine("Serial:  " + (DateTime.Now - t0));
        }
    }

    
}
