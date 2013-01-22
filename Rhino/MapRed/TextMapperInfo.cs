﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.MapRed
{
    /// <summary>
    /// Contains information about the map phase.
    /// </summary>
    public class TextMapperInfo
    {
        public long MapEmits = 0;
        public long ProcessedRecords = 0;
        public long ProcessedChars = 0;
        public long SpilledBytes = 0;
        public long SpilledRecords = 0;
    }
}
