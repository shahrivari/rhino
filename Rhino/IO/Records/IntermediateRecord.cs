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

            int consumed_val = 0;
            foreach (var val in values)
            {
                consumed_val++;
                var val_bytes = val_serializer.Invoke(val);
                byte_seq.AddRange(BitConverter.GetBytes(val_bytes.Length));
                byte_seq.AddRange(val_bytes);
                if (byte_seq.Count >= maxChunkSize)
                {
                    if (consumed_val == values.Count)
                        byte_seq.Add(0);
                    else
                        byte_seq.Add(1);
                    var val_list_bytes = byte_seq.ToArray();
                    byte_seq.Clear();
                    result.Add(ArrayUtils.Combine(BitConverter.GetBytes(key_bytes.Length), key_bytes, BitConverter.GetBytes(val_list_bytes.Length), val_list_bytes));
                }
            }

            if (byte_seq.Count > 0)
            {
                byte_seq.Add(0);
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

        public static byte ReadValueList(Stream stream, out List<InterVal> values)
        {
            values = new List<InterVal>();

            var list_len = ReadValueListLength(stream);

            long read_bytes = 0;

            var val_deserilaizer = Serialization.GetBinaryDeserializer(typeof(InterVal));
            var record_len_bytes = new byte[sizeof(int)];
            int record_len = 0;
            var record_buffer = new byte[4096];
            InterVal value;
            while (read_bytes < list_len - sizeof(byte))
            {
                read_bytes += stream.Read(record_len_bytes, 0, record_len_bytes.Length);
                record_len = BitConverter.ToInt32(record_len_bytes, 0);
                //if (record_len > record_buffer.Length)
                //    record_buffer = new byte[record_len];
                record_buffer = new byte[record_len];
                read_bytes += stream.Read(record_buffer, 0, record_len);
                value = (InterVal)val_deserilaizer.Invoke(record_buffer,0);
                values.Add(value);
            }

            return (byte)stream.ReadByte();
        }

        public static byte ReadRecord(Stream stream,out KeyValuePair<InterKey, List<InterVal>> pair)
        {
            var key = ReadKey(stream);
            var list = new List<InterVal>();
            var res = ReadValueList(stream, out list);
            pair = new KeyValuePair<InterKey, List<InterVal>>(key, list);
            return res;
        }

    }
}
