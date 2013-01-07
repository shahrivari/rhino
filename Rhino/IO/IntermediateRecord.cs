using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public class IntermediateRecord
    {
        byte[] bytes;

        public byte[] Bytes
        {
          get { return bytes; }
        }

        public IntermediateRecord(byte[] obj)
        {
            var len_bytes=BitConverter.GetBytes(obj.Length);
            bytes = new byte[obj.Length + len_bytes.Length];
            Buffer.BlockCopy(len_bytes, 0, bytes, 0, len_bytes.Length);
            Buffer.BlockCopy(obj, 0, bytes, len_bytes.Length, obj.Length);
        }
    }
}
