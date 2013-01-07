using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Util
{
    public class StringFormatter
    {
        public static string HumanReadable(double val)
        {
            string[] sizes = { "", "Kilo", "Mega", "Giga" };
            int order = 0;
            while (val >= 1024 && order + 1 < sizes.Length)
            {
                order++;
                val = val / 1024;
            }
            // Adjust the format string to your preferences. For example "{0:0.#}{1}" would
            // show a single decimal place, and no space.
            return String.Format("{0:0.##} {1}", val, sizes[order]);
        }
    }
}
