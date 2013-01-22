using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Rhino.MapRed
{
    /// <summary>
    /// Represents a context for gathering reduce outputs.
    /// </summary>
    public class ReduceContext
    {
        long emitCount = 0;

        /// <summary>
        /// Number of emmitions done yet.
        /// </summary>
        public long EmitCount
        {
            get { return emitCount; }
        }

        StreamWriter writer;

        /// <summary>
        /// Constructs a reduce context given a stream_writer
        /// </summary>
        /// <param name="writer">the stream_writer to write outputs using it.</param>
        public ReduceContext(StreamWriter writer)
        {
            this.writer = writer;
        }

        /// <summary>
        /// Outputs a string.
        /// </summary>
        /// <param name="str">the string to output</param>
        public void Emit(String str)
        {
            writer.WriteLine(str);
            emitCount++;
        }
    }
}
