using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino
{
    public interface IMapReduceContext<EmitKey,EmitVal>
    {
        void Emit(EmitKey key,EmitVal val);
    }
}
