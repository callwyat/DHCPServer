using System;
using System.Collections.Generic;
using System.Text; 
using System.IO;
using System.Linq;

namespace GitHub.JPMikkers.DHCP
{
    public class DHCPOptionUserClassIdentifier : DHCPOptionBase
    {
        private byte[] _data;

        public byte[] Data
        {
            get { return _data; }
            set { _data = value; }
        }
        public string[] UserClasses
        {
            get
            {
                List<string> _userclasses = new List<string>();
                for(int i = 0; i < _data.Length;)
                {
                    int len = _data[i++];
                    if ( i + len > _data.Length ) { break; }
                    _userclasses.Add(Encoding.ASCII.GetString(_data, i, len));
                    i += len;
                }
                return _userclasses.ToArray();
            }
        }

        #region IDHCPOption Members

        public override IDHCPOption FromStream(Stream s)
        {
            DHCPOptionUserClassIdentifier result = new DHCPOptionUserClassIdentifier();
            result._data = new byte[s.Length];
            s.Read(result._data, 0, result._data.Length);
            return result;
        }

        public override void ToStream(Stream s)
        {
            s.Write(_data, 0, _data.Length);
        }

        #endregion

        public DHCPOptionUserClassIdentifier()
            : base(TDHCPOption.UserClassIdentifier)
        {
            _data = new byte[0];
        }

        public DHCPOptionUserClassIdentifier(byte[] data)
            : base(TDHCPOption.UserClassIdentifier)
        {
            _data = data;
        }

        public override string ToString()
        {
            string classes = String.Join(",", UserClasses);
            return $"Option(name=[{OptionType}],value=[{classes}])";
        }
    }
}
