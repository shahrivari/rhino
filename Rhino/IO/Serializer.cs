using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public static class Serializer
    {
        static Dictionary<Type, Func<object, byte[]>> binarySerializers = new Dictionary<Type, Func<object, byte[]>>();
        static Dictionary<Type, Func<byte[], object>> binaryDeserializers = new Dictionary<Type, Func<byte[], object>>();

        static Serializer()
        {
            //serializers for basic types
            binarySerializers.Add(typeof(bool), (val) => { return BitConverter.GetBytes((bool)val); });
            binaryDeserializers.Add(typeof(bool), (val) => { return BitConverter.ToBoolean(val, 0); });

            binarySerializers.Add(typeof(char), (val) => { return BitConverter.GetBytes((char)val); });
            binaryDeserializers.Add(typeof(char), (val) => { return BitConverter.ToChar(val, 0); });
            
            binarySerializers.Add(typeof(Int16), (val) => { return BitConverter.GetBytes((short)val); });
            binaryDeserializers.Add(typeof(Int16), (val) => { return BitConverter.ToInt16(val, 0); });
            
            binarySerializers.Add(typeof(Int32), (val) => { return BitConverter.GetBytes((int)val);});
            binaryDeserializers.Add(typeof(Int32), (val) => { return BitConverter.ToInt32(val, 0); });
            
            binarySerializers.Add(typeof(Int64), (val) => { return BitConverter.GetBytes((long)val); });
            binaryDeserializers.Add(typeof(Int64), (val) => { return BitConverter.ToInt64(val, 0); });
            
            binarySerializers.Add(typeof(double), (val) => { return BitConverter.GetBytes((double)val); });
            binaryDeserializers.Add(typeof(double), (val) => { return BitConverter.ToDouble(val, 0); });
            
            binarySerializers.Add(typeof(float), (val) => { return BitConverter.GetBytes((float)val); });
            binaryDeserializers.Add(typeof(float), (val) => { return BitConverter.ToSingle(val, 0); });
            
            binarySerializers.Add(typeof(string), (val) =>
            {
                string str = (string)val;
                byte[] bytes = new byte[str.Length * sizeof(char)];
                System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
                return bytes;
            });
            binaryDeserializers.Add(typeof(string), (bytes) =>
            {
                char[] chars = new char[bytes.Length / sizeof(char)];
                System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
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

        public static Func<byte[] , object> GetBinaryDeserializer(Type t)
        {
            if (!binaryDeserializers.ContainsKey(t))
                throw new ArgumentException("Type '" + t.ToString() + "' was not registered for serialization!");
            return binaryDeserializers[t];
        }

    }
}
