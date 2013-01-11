using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Rhino.Util;

namespace Rhino.IO
{
    public static class Serializer
    {
        static Dictionary<Type, Func<object, byte[]>> binarySerializers = new Dictionary<Type, Func<object, byte[]>>();
        static Dictionary<Type, Func<byte[],int, object>> binaryDeserializers = new Dictionary<Type, Func<byte[],int, object>>();



        public static void RegisterBasicTypes()
        {
            //serializers for basic types
            binarySerializers.Add(typeof(bool), (val) => { return BitConverter.GetBytes((bool)val).Reverse().ToArray(); });
            binaryDeserializers.Add(typeof(bool), (bytes, index) => { return BitConverter.ToBoolean(bytes, index); });

            binarySerializers.Add(typeof(char), (val) => { return BitConverter.GetBytes((char)val); });
            binaryDeserializers.Add(typeof(char), (bytes, index) => { return BitConverter.ToChar(bytes, index); });
            
            binarySerializers.Add(typeof(Int16), (val) => { return BitConverter.GetBytes((short)val); });
            binaryDeserializers.Add(typeof(Int16), (bytes, index) => { return BitConverter.ToInt16(bytes, index); });
            
            binarySerializers.Add(typeof(Int32), (val) => { return BitConverter.GetBytes((int)val);});
            binaryDeserializers.Add(typeof(Int32), (bytes, index) => { return BitConverter.ToInt32(bytes, index); });
            
            binarySerializers.Add(typeof(Int64), (val) => { return BitConverter.GetBytes((long)val); });
            binaryDeserializers.Add(typeof(Int64), (bytes, index) => { return BitConverter.ToInt64(bytes, index); });
            
            binarySerializers.Add(typeof(double), (val) => { return BitConverter.GetBytes((double)val); });
            binaryDeserializers.Add(typeof(double), (bytes, index) => { return BitConverter.ToDouble(bytes, index); });
            
            binarySerializers.Add(typeof(float), (val) => { return BitConverter.GetBytes((float)val); });
            binaryDeserializers.Add(typeof(float), (bytes, index) => { return BitConverter.ToSingle(bytes, index); });
            
            binarySerializers.Add(typeof(string), (val) =>
            {
                string str = (string)val;
                byte[] bytes = new byte[str.Length * sizeof(char)];
                System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
                return bytes;
            });
            binaryDeserializers.Add(typeof(string), (bytes, index) =>
            {
                char[] chars = new char[bytes.Length / sizeof(char)];
                System.Buffer.BlockCopy(bytes, index, chars, 0, bytes.Length);
                return new string(chars);
            });
        }

        public static void RegisterBinarySerializer(Type t, Func<object, byte[]> func)
        {
            if (binarySerializers.ContainsKey(t))
                throw new ArgumentException("Type '" + t.ToString() + "' was registered before!");
            else
                binarySerializers[t] = func;
        }

        public static Func<object, byte[]> GetBinarySerializer(Type t)
        {
            if(!binarySerializers.ContainsKey(t))
                throw new ArgumentException("Type '"+t.ToString()+"' was not registered for serialization!");
            return binarySerializers[t]; 
        }

        public static Func<byte[], int , object> GetBinaryDeserializer(Type t)
        {
            if (!binaryDeserializers.ContainsKey(t))
                throw new ArgumentException("Type '" + t.ToString() + "' was not registered for serialization!");
            return binaryDeserializers[t];
        }

        public static SmallRecord BinarySerializeToSmallRecord(object obj)
        {
            var serializer = GetBinarySerializer(obj.GetType());
            var bytes = serializer.Invoke(obj);
            return new SmallRecord(bytes);
        }

        public static byte[] BinarySerializeIntermediateList<T>(IEnumerable<T> list)
        {
            var serializer = GetBinarySerializer(typeof(T));
            List<byte> byte_seq = new List<byte>();
            //MemoryStream stream = new MemoryStream(SmallRecord.MaxLength);
            foreach (var x in list)
            {
                var small_record=BinarySerializeToSmallRecord(x);
                byte_seq.AddRange(small_record.Bytes);
                //stream.Write(small_record.Bytes, 0, small_record.Bytes.Length);
            }

            var result = ArrayUtils.Combine(BitConverter.GetBytes(byte_seq.Count), byte_seq.ToArray());
            return result;
            //return new SmallRecord(stream.ToArray());
        }

    }
}
