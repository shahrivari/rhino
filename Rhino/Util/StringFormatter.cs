using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Rhino.Util
{
    /// <summary>
    /// Presents a set of static methods for formatting numbers.
    /// </summary>
    public static class StringFormatter
    {
        /// <summary>
        /// Rounds to two precision real number and appends Kilo, Mega, and Giga postficies.
        /// Very nice for logging and printing bytes count.
        /// </summary>
        /// <param name="val">the value for pretify</param>
        /// <returns>a string having the postfix</returns>
        public static string HumanReadablePostfixs(double val)
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

        /// <summary>
        /// Adds comma digit grouping for better number reading.
        /// </summary>
        /// <param name="val">the value to be formatted</param>
        /// <returns>the formatted string</returns>
        public static string DigitGrouped(long val)
        {
            return val.ToString("N");
        }
    }
}
