using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;

namespace GitHub.JPMikkers.DHCP
{
    public class DHCPTraceEventArgs : EventArgs
    {
        private string m_Message;
        public string Message { get { return m_Message; } set { m_Message = value; } }
    }

    public class DHCPStopEventArgs : EventArgs
    {
        public Exception? Reason { get; set; }

        public new static DHCPStopEventArgs Empty { get; } = new();
    }

    public enum OptionMode
    {
        Default,
        Force
    }

    public struct OptionItem
    {
        public OptionMode Mode;
        public IDHCPOption Option;

        public OptionItem(OptionMode mode, IDHCPOption option)
        {
            this.Mode = mode;
            this.Option = option;
        }
    }

    public class ReservationItem
    {
        private static readonly Regex _regex = new(@"^(?<mac>([0-9a-fA-F][0-9a-fA-F][:\-\.]?)+)(?<netmask>/[0-9]+)?", RegexOptions.Compiled | RegexOptions.ExplicitCapture);
        private string _macTaste;
        private byte[] _prefix;
        private UInt32 _prefixBits;

        public string MacTaste
        {
            get => _macTaste;

            set
            {
                _macTaste = value;
                _prefix = null;
                _prefixBits = 0;

                if(!string.IsNullOrWhiteSpace(MacTaste))
                {
                    try
                    {
                        Match match = _regex.Match(_macTaste);
                        if(match.Success && match.Groups["mac"].Success)
                        {
                            _prefix = Utils.HexStringToBytes(match.Groups["mac"].Value);
                            _prefixBits = (uint)_prefix.Length * 8;

                            if(match.Groups["netmask"].Success)
                            {
                                _prefixBits = UInt32.Parse(match.Groups["netmask"].Value.Substring(1));
                            }
                        }
                    }
                    catch
                    {
                    }
                }
            }
        }

        public string HostName { get; set; }
        public IPAddress PoolStart { get; set; }
        public IPAddress PoolEnd { get; set; }
        public bool Preempt { get; set; }

        private static bool MacMatch(byte[] mac, byte[] prefix, uint bits)
        {
            // prefix should have more bits than masklength
            if(((bits + 7) >> 3) > prefix.Length) 
            {
                return false;
            }

            // prefix should be shorter or equal to mac address
            if(prefix.Length > mac.Length) 
            {
                return false;
            }

            for(int t = 0; t < (bits - 7); t += 8)
            {
                if(mac[t >> 3] != prefix[t >> 3]) 
                {
                    return false;
                }
            }

            if((bits & 7) > 0)
            {
                byte bitMask = (byte)(0xFF00 >> ((int)bits & 7));
                if((mac[bits >> 3] & bitMask) != (prefix[bits >> 3] & bitMask)) 
                {
                    return false;
                }
            }
            return true;
        }

        public bool Match(DHCPMessage message)
        {
            DHCPClient client = DHCPClient.CreateFromMessage(message);

            if(!string.IsNullOrWhiteSpace(MacTaste) && _prefix != null)
            {
                return MacMatch(client.HardwareAddress, _prefix, _prefixBits);
            }
            else if(!string.IsNullOrWhiteSpace(HostName))
            {
                if(!string.IsNullOrWhiteSpace(client.HostName))
                {
                    if(client.HostName.StartsWith(HostName, true, CultureInfo.InvariantCulture))
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /// <summary>
    /// The interface of a DHCPServer
    /// </summary>
    public interface IDHCPServer : IDisposable
    {
        /// <summary>
        /// Event that is raised when the status of the server changed
        /// </summary>
        event EventHandler<DHCPStopEventArgs> OnStatusChange;

        /// <summary>
        /// The address to host the DHCP Server from
        /// </summary>
        IPEndPoint EndPoint { get; set; }

        /// <summary>
        /// The "SubnetMask" to assign to any requesting clients
        /// </summary>
        IPAddress SubnetMask { get; set; }

        /// <summary>
        /// The first address available to assign to a requesting client
        /// </summary>
        IPAddress PoolStart { get; set; }

        /// <summary>
        /// The last address available to assign a requesting client
        /// </summary>
        IPAddress PoolEnd { get; set; }

        /// <summary>
        /// How long the client has to accept the request
        /// </summary>
        TimeSpan OfferExpirationTime { get; set; }

        /// <summary>
        /// How long the client can use the address for
        /// </summary>
        TimeSpan LeaseTime { get; set; }

        /// <summary>
        /// A list of all clients the server has served
        /// </summary>
        IReadOnlyList<DHCPClient> Clients { get; }

        /// <summary>
        /// The name of the host of the server
        /// </summary>
        string HostName { get; }

        /// <summary>
        /// A boolean indicating if the server is active or not
        /// </summary>
        bool Active { get; }

        /// <summary>
        /// A collection of all the options the server can provide
        /// </summary>
        List<OptionItem> Options { get; set; }

        /// <summary>
        /// Clients that should always get the same information
        /// </summary>
        List<ReservationItem> Reservations { get; set; }

        /// <summary>
        /// Starts the DHCP Server
        /// </summary>
        void Start();

        /// <summary>
        /// Stops the DHCP Server
        /// </summary>
        void Stop();
    }
}