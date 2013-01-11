using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO.Records
{
    public class KeyPositionRecord<T>
    {
        T key;
        int valPos;

        public KeyPositionRecord(T key, int val_pos)
        {
            this.key = key;
            valPos = val_pos;
        }

        public static byte[] GetBytes<T>(T key, int values_pos, Func<object, byte[]> serialization_func=null)
        {
            if (serialization_func == null)
                serialization_func = Serializer.GetBinarySerializer(typeof(T));

            var key_bytes=serialization_func.Invoke(key);
            byte[] result = new byte[key_bytes.Length + sizeof(int)];
            Buffer.BlockCopy(key_bytes, 0, result, 0, key_bytes.Length);            
            var pos_bytes = BitConverter.GetBytes(values_pos);
            Buffer.BlockCopy(pos_bytes, 0, result, key_bytes.Length, pos_bytes.Length);
            return result;
        }

    }
}
