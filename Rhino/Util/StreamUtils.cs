using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.Util
{
    /// <summary>
    /// Presents a set of static methods for working with streams.
    /// </summary>
    /// <remarks>
    /// Currently just has methods for copying.
    /// </remarks>
    public static class StreamUtils
    {

        static byte[] buffer = new byte[32 * 1024];
        /// <summary>
        /// Copies from a source stream into a destination stream.
        /// </summary>
        /// <param name="input">the input stream</param>
        /// <param name="output">the output stream</param>
        /// <param name="count">the counts of bytes to copy</param>
        /// <returns>the count of the byes copied</returns>
        /// <remarks>uses a shared buffer inorder to prevent reallocations so it is not safe to call this method in multi-threaded execution!</remarks>
        public static long Copy(Stream input, Stream output, long count)
        {            
            long remaining=count;
            int read;
            while (true)
            {
                if (remaining <= buffer.Length)
                {
                    read = input.Read(buffer, 0, (int)remaining);
                    remaining-=read;
                    output.Write(buffer, 0, read);
                    break;
                }
                else
                {
                    read = input.Read(buffer, 0, buffer.Length);
                    remaining-=read;
                    output.Write(buffer, 0, read);
                }
            }

            return count - remaining;
        }
    }
}
