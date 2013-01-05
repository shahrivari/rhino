using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Rhino;
using Rhino.IO;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            TextFileInputReader reader = new TextFileInputReader(@"X:\alaki\BIN_NORM_20_10M.csv");
            while (true)
            {
                if (reader.ReadRecord() == null)
                    break;
            }
        }            
    }
}
