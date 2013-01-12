using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Util
{
    public class ArrayUtils
    {
        public static byte[] Combine(byte[] array1, byte[] array2)
        {
            byte[] result = new byte[array1.Length + array2.Length];
            Buffer.BlockCopy(array1, 0, result, 0, array1.Length);
            Buffer.BlockCopy(array2, 0, result, array1.Length, array2.Length);
            return result;
        }

        public static byte[] Combine(byte[] array1, byte[] array2, byte[] array3)
        {
            byte[] result = new byte[array1.Length + array2.Length + array3.Length];
            Buffer.BlockCopy(array1, 0, result, 0, array1.Length);
            Buffer.BlockCopy(array2, 0, result, array1.Length, array2.Length);
            Buffer.BlockCopy(array3, 0, result, array1.Length + array2.Length, array3.Length);
            return result;
        }

        public static byte[] Combine(byte[] array1, byte[] array2, byte[] array3, byte[] array4)
        {
            byte[] result = new byte[array1.Length + array2.Length + array3.Length + array4.Length];
            Buffer.BlockCopy(array1, 0, result, 0, array1.Length);
            Buffer.BlockCopy(array2, 0, result, array1.Length, array2.Length);
            Buffer.BlockCopy(array3, 0, result, array1.Length + array2.Length, array3.Length);
            Buffer.BlockCopy(array4, 0, result, array1.Length + array2.Length + array3.Length, array4.Length);
            return result;
        }

    }
}
