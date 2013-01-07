using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO
{
    public static class Serializer
    {
        static Dictionary<Type, Func<object, byte[]>> binarySerializers = new Dictionary<Type, Func<object, byte[]>>();

        static Serializer()
        {
            //serializers for basic types
            binarySerializers.Add(typeof(int), (val) => { return BitConverter.GetBytes((int)val);});
        }

        public static Func<object, byte[]> GetBinarySerializer(Type t)
        {
            return null; 
        }
    }
}
