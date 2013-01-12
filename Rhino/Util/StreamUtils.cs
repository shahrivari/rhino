using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.Util
{
    public static class StreamUtils
    {
        static byte[] buffer = new byte[32 * 1024];

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
