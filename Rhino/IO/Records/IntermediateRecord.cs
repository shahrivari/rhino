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
        public static byte[] GetIntermediateRecordBytes(InterKey key, List<InterVal> values)
        {
            var key_serializer = Serialization.GetBinarySerializer(typeof(InterKey));
            var val_serializer = Serialization.GetBinarySerializer(typeof(InterVal));
            
            var key_bytes = key_serializer.Invoke(key);

            List<byte> byte_seq = new List<byte>();
            
            foreach (var val in values)
            {
                var val_bytes = val_serializer.Invoke(val);
                byte_seq.AddRange(BitConverter.GetBytes(val_bytes.Length));
                byte_seq.AddRange(val_bytes);
            }
            var val_list_bytes=byte_seq.ToArray();

            return ArrayUtils.Combine(BitConverter.GetBytes(key_bytes.Length), key_bytes, BitConverter.GetBytes(val_list_bytes.LongLength), val_list_bytes);           
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
            var long_bytes = new byte[sizeof(long)];
            stream.Read(long_bytes, 0, long_bytes.Length);
            var list_len = BitConverter.ToInt64(long_bytes, 0);
            return list_len;
        }


        //private static List<InterVal> readValues(Stream stream)
        //{
        //    List<InterVal> list = new List<InterVal>();
        //    var long_bytes = new byte[sizeof(long)];
        //    stream.Read(long_bytes, 0, long_bytes.Length);
        //    var list_len = BitConverter.ToInt32(long_bytes, 0);
        //    int read_bytes = 0;
        //    while (read_bytes < list_len)
        //    {
        //        var int_bytes = new byte[sizeof(int)];
        //        read_bytes+= stream.Read(int_bytes, 0, int_bytes.Length);
        //        var val_len = BitConverter.ToInt32(int_bytes, 0);
        //        var val_bytes = new byte[val_len];
        //        read_bytes += stream.Read(val_bytes, 0, val_len);
        //        list.Add((InterVal)Serialization.GetBinaryDeserializer(typeof(InterVal)).Invoke(val_bytes,0));
        //    }
        //    return list;
        //}

    }
}
