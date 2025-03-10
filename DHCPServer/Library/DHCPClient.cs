using System;
using System.Net;
using System.Xml.Serialization;

namespace GitHub.JPMikkers.DHCP
{
    [Serializable]
    public class DHCPClient : IEquatable<DHCPClient>
    {
        public enum TState
        {
            Released,
            Offered,
            Assigned
        }

        private IPAddress _iPAddress = IPAddress.Any;
        private DateTime _offeredTime;
        private DateTime _leaseStartTime;
        private TimeSpan _leaseDuration;

        [XmlElement(DataType = "hexBinary")]
        public byte[] Identifier { get; set; } = [];

        [XmlElement(DataType = "hexBinary")]
        public byte[] HardwareAddress { get; set; } = [];

        public string HostName { get; set; } = string.Empty;

        public TState State {get; set; } = TState.Released;

        [XmlIgnore]
        internal DateTime OfferedTime
        {
            get { return _offeredTime; }
            set { _offeredTime = value; }
        }

        public DateTime LeaseStartTime
        {
            get { return _leaseStartTime; }
            set { _leaseStartTime = value; }
        }

        [XmlIgnore]
        public TimeSpan LeaseDuration
        {
            get { return _leaseDuration; }
            set { _leaseDuration = Utils.SanitizeTimeSpan(value); }
        }

        public DateTime LeaseEndTime
        {
            get
            {
                return Utils.IsInfiniteTimeSpan(_leaseDuration) ? DateTime.MaxValue : (_leaseStartTime + _leaseDuration);
            }
            set
            {
                if(value >= DateTime.MaxValue)
                {
                    _leaseDuration = Utils.InfiniteTimeSpan;
                }
                else
                {
                    _leaseDuration = value - _leaseStartTime;
                }
            }
        }

        [XmlIgnore]
        public IPAddress IPAddress
        {
            get { return _iPAddress; }
            set { _iPAddress = value; }
        }

        [XmlElement(ElementName = "IPAddress")]
        public string IPAddressAsString
        {
            get { return _iPAddress.ToString(); }
            set { _iPAddress = IPAddress.Parse(value); }
        }

        public DHCPClient()
        {
        }

        public DHCPClient Clone()
        {
            return new()
            {
                Identifier = Identifier,
                HardwareAddress = HardwareAddress,
                HostName = HostName,
                State = State,
                _iPAddress = _iPAddress,
                _offeredTime = _offeredTime,
                _leaseStartTime = _leaseStartTime,
                _leaseDuration = _leaseDuration
            };;
        }

        internal static DHCPClient CreateFromMessage(DHCPMessage message)
        {
            DHCPClient result = new()
            {
                HardwareAddress = message.ClientHardwareAddress
            };

            DHCPOptionHostName dhcpOptionHostName = (DHCPOptionHostName)message.GetOption(TDHCPOption.HostName);

            if(dhcpOptionHostName != null)
            {
                result.HostName = dhcpOptionHostName.HostName;
            }

            DHCPOptionClientIdentifier dhcpOptionClientIdentifier = (DHCPOptionClientIdentifier)message.GetOption(TDHCPOption.ClientIdentifier);

            if(dhcpOptionClientIdentifier != null)
            {
                result.Identifier = dhcpOptionClientIdentifier.Data;
            }
            else
            {
                result.Identifier = message.ClientHardwareAddress;
            }

            return result;
        }

        #region IEquatable and related

        public override bool Equals(object obj)
        {
            return obj is DHCPClient other && Utils.ByteArraysAreEqual(Identifier, other.Identifier);
        }

        bool IEquatable<DHCPClient>.Equals(DHCPClient other)
        {
            return other is not null && Utils.ByteArraysAreEqual(Identifier, other.Identifier);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = 0;
                foreach(byte b in Identifier) 
                {
                    result = (result * 31) ^ b;
                }
                return result;
            }
        }

        #endregion

        public override string ToString()
        {
            return $"{Utils.BytesToHexString(Identifier, "-")} ({HostName})";
        }
    }
}
