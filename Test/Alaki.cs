using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Test
{
    class Alaki
    {
        Type inkey;
        Type inval;

        void setMap<Key,Val,InterKey,InterVal>(Func<Key,Val,KeyValuePair<InterKey,InterVal>> g) 
        {
            inkey = typeof(Key);
            inval = typeof(Val);
            Type tt = typeof(List<>);
            Type gg=tt.MakeGenericType(inkey);
            object o=Activator.CreateInstance(gg);
            gg.GetMethod("Add").Invoke(o, new object[]{1});
            
        }

        public static void xx()
        {
            Alaki aa=new Alaki();
            aa.setMap<int, int, int, int>((x, y) => { return new KeyValuePair<int, int>(x, y); });
        }
    }

    
}
