using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Util
{
    /// <summary>
    /// Presents a set of static methods to work with arrays.
    /// </summary>
    /// <remarks>
    /// Currently just has methods for byte arrays.
    /// </remarks>
    public static class ArrayUtils
    {
        /// <summary>
        /// Concats two arrays into a single array.
        /// </summary>
        /// <param name="array1">the first array</param>
        /// <param name="array2">the second array</param>
        /// <returns>a byte array containing concatenation of both inputs</returns>
        public static byte[] Concat(byte[] array1, byte[] array2)
        {
            byte[] result = new byte[array1.Length + array2.Length];
            Buffer.BlockCopy(array1, 0, result, 0, array1.Length);
            Buffer.BlockCopy(array2, 0, result, array1.Length, array2.Length);
            return result;
        }

        /// <summary>
        /// Concats three arrays into a single array.
        /// </summary>
        /// <param name="array1">the first array</param>
        /// <param name="array2">the second array</param>
        /// <param name="array3">the last array</param>
        /// <returns>a byte array containing concatenation of both inputs</returns>
        public static byte[] Concat(byte[] array1, byte[] array2, byte[] array3)
        {
            byte[] result = new byte[array1.Length + array2.Length + array3.Length];
            Buffer.BlockCopy(array1, 0, result, 0, array1.Length);
            Buffer.BlockCopy(array2, 0, result, array1.Length, array2.Length);
            Buffer.BlockCopy(array3, 0, result, array1.Length + array2.Length, array3.Length);
            return result;
        }

        /// <summary>
        /// Concats four arrays into a single array.
        /// </summary>
        /// <param name="array1">the first array</param>
        /// <param name="array2">the second array</param>
        /// <param name="array3">the third array</param>
        /// <param name="array4">the last array</param>
        /// <returns>a byte array containing concatenation of both inputs</returns>
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
