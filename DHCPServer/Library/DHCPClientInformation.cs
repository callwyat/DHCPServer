using System;
using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;

namespace GitHub.JPMikkers.DHCP
{
    [Serializable()]
    public class DHCPClientInformation
    {
        public DateTime TimeStamp
        {
            get
            {
                return DateTime.Now;
            }
            set
            {
            }
        }

        public List<DHCPClient> Clients { get; set; } = [];

        private static readonly XmlSerializer _serializer = new(typeof(DHCPClientInformation));

        public static DHCPClientInformation Read(string file)
        {
            DHCPClientInformation result;

            if(File.Exists(file))
            {
                using(Stream s = File.OpenRead(file))
                {
                    result = (DHCPClientInformation)_serializer.Deserialize(s);
                }
            }
            else
            {
                result = new DHCPClientInformation();
            }

            return result;
        }

        public void Write(string file)
        {
            string dirName = Path.GetDirectoryName(file);

            if(!string.IsNullOrEmpty(dirName) && !Directory.Exists(dirName))
            {
                Directory.CreateDirectory(dirName);
            }

            using(Stream s = File.Open(file, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            {
                _serializer.Serialize(s, this);
                s.Flush();
            }
        }
    }
}
