using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Old
{
    public interface IMapContext<EmitKey,EmitVal>
    {
        void Emit(EmitKey key,EmitVal val);
    }
}
