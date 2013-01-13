using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.Util;
using System.IO;

namespace Rhino.IO.Records
{
    public class IntermediateRecord<InterKey,InterVal>
    {
        static int maxChunkSize = 4 * 1024 * 1024;

        public static List<byte[]> GetIntermediateRecordBytes(InterKey key, List<InterVal> values)
        {
            List<byte[]> result=new List<byte[]>();
            var key_serializer = Serialization.GetBinarySerializer(typeof(InterKey));
            var val_serializer = Serialization.GetBinarySerializer(typeof(InterVal));
            
            var key_bytes = key_serializer.Invoke(key);

            List<byte> byte_seq = new List<byte>();
            
            foreach (var val in values)
            {
                var val_bytes = val_serializer.Invoke(val);
                byte_seq.AddRange(BitConverter.GetBytes(val_bytes.Length));
                byte_seq.AddRange(val_bytes);
                if (byte_seq.Count >= maxChunkSize)
                {
                    var val_list_bytes = byte_seq.ToArray();
                    byte_seq.Clear();
                    result.Add(ArrayUtils.Combine(BitConverter.GetBytes(key_bytes.Length), key_bytes, BitConverter.GetBytes(val_list_bytes.Length), val_list_bytes));
                }
            }

            if (byte_seq.Count > 0)
            {
                var val_list_bytes = byte_seq.ToArray();
                result.Add(ArrayUtils.Combine(BitConverter.GetBytes(key_bytes.Length), key_bytes, BitConverter.GetBytes(val_list_bytes.Length), val_list_bytes));
            }

            return result;            
        }

        public static InterKey ReadKey(Stream stream)
        {
            var int_bytes = new byte[sizeof(int)];
            stream.Read(int_bytes, 0, int_bytes.Length);
            var key_len = BitConverter.ToInt32(int_bytes, 0);
            var key_bytes = new byte[key_len];
            stream.Read(key_bytes, 0, key_len);
            return (InterKey)Serialization.GetBinaryDeserializer(typeof(InterKey)).Invoke(key_bytes,0);
        }
        
        public static long ReadValueListLength(Stream stream)
        {
            var int_bytes = new byte[sizeof(int)];
            stream.Read(int_bytes, 0, int_bytes.Length);
            var list_len = BitConverter.ToInt32(int_bytes, 0);
            return list_len;
        }

    }
}
