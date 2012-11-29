using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public abstract class MapReduceBase<InKey, InValue, InterKey, InterValue, OutKey, OutValue>
    {
        abstract protected IEnumerable<KeyValuePair<InterKey,InterValue>> map(InKey key, InValue value);
        abstract protected KeyValuePair<OutKey, OutValue> reduce(InterKey key, IEnumerable<InterValue> values);
    }
}
