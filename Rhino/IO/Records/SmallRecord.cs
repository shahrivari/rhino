using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.IO.Records
{
    public class SmallRecord : IComparable
    {
        static int maxLength = 256 * 1024 * 1024;
        public static int MaxLength
        {
            get { return SmallRecord.maxLength; }
            set { SmallRecord.maxLength = value; }
        }


        byte[] bytes;
        public byte[] Bytes
        {
            get { return bytes; }
        }

        public int Length
        {
            get { return bytes.Length; }
        }

        public SmallRecord(byte[] obj)
        {
            if (obj.Length > maxLength)
                throw new ArgumentException("Object length(" + obj.Length + ") is bigger than the default max(" + MaxLength + ").");
            var len_bytes = BitConverter.GetBytes(obj.Length);
            bytes = new byte[obj.Length + len_bytes.Length];
            Buffer.BlockCopy(len_bytes, 0, bytes, 0, len_bytes.Length);
            Buffer.BlockCopy(obj, 0, bytes, len_bytes.Length, obj.Length);
        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;

            SmallRecord other_record = obj as SmallRecord;
            int index = 0;
            int last = Math.Min(this.Bytes.Length, other_record.Bytes.Length);

            if (other_record != null)
            {
                for (index = 0; index < last; index++)
                {
                    if (this.bytes[index] < other_record.bytes[index])
                        return -1;
                    else
                        if (this.bytes[index] > other_record.bytes[index])
                            return 1;
                }
                if (this.bytes.Length < other_record.bytes.Length)
                    return -1;
                else
                    if (this.bytes.Length > other_record.bytes.Length)
                        return 1;
                    else
                        return 0;
            }
            else
                throw new ArgumentException("Object is not a SmallRecord");
        }
    }
}
