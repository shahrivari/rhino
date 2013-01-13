using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Util
{
    public class PQ<TPriority, TValue>
    {
        LinkedList<KeyValuePair<TPriority, TValue>> list = new LinkedList<KeyValuePair<TPriority, TValue>>();
        Comparer<TPriority> comparer= Comparer<TPriority>.Default;

        public void Enqueue(TPriority priority, TValue value)
        {
            list.AddLast(new KeyValuePair<TPriority, TValue>(priority, value));
        }

        public KeyValuePair<TPriority, TValue> Dequeue()
        {
            if(list.Count==0)
                throw new InvalidOperationException("Priority queue is empty");

            var min = list.First;

            for (var node = list.First; node != list.Last; node = node.Next)
                if (comparer.Compare(node.Value.Key , min.Value.Key)<0)
                    min = node;
            list.Remove(min);
            return min.Value;            
        }

        public int Count
        {
            get { return list.Count; }
        }

    }
}
