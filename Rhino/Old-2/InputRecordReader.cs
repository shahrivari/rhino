﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Old
{
    public interface InputRecordReader<Key,Value>
    {
        KeyValuePair<Key,Value> readNextRecord();
        bool HasNextRecord();
    }
}
