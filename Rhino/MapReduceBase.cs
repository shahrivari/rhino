using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public interface IMapReduce<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        void map(InKey key, InValue value, IMapReduceContext<InterKey, InterValue> context);
        void reduce(InterKey key, IEnumerable<InterValue> values, IMapReduceContext<OutKey, OutValue> context);
    }
}
