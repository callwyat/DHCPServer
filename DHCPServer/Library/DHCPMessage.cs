using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace GitHub.JPMikkers.DHCP
{
    public class DHCPMessage
    {
        private static readonly IDHCPOption[] s_optionsTemplates;

        public enum TOpcode
        {
            Unknown = 0,
            BootRequest = 1,
            BootReply = 2
        }

        public enum THardwareType
        {
            Unknown = 0,
            Ethernet = 1,
            Experimental_Ethernet = 2,
            Amateur_Radio_AX_25 = 3,
            Proteon_ProNET_Token_Ring = 4,
            Chaos = 5,
            IEEE_802_Networks = 6,
            ARCNET = 7,
            Hyperchannel = 8,
            Lanstar = 9,
            Autonet_Short_Address = 10,
            LocalTalk = 11,
            LocalNet = 12,
            Ultra_link = 13,
            SMDS = 14,
            Frame_Relay = 15,
            Asynchronous_Transmission_Mode1 = 16,
            HDLC = 17,
            Fibre_Channel = 18,
            Asynchronous_Transmission_Mode2 = 19,
            Serial_Line = 20,
            Asynchronous_Transmission_Mode3 = 21,
        };

        public TOpcode Opcode { get; set; }

        public THardwareType HardwareType { get; set; }

        public byte Hops { get; set; }

        public uint XID { get; set; }

        public ushort Secs { get; set; }

        public bool BroadCast { get; set; }

        public IPAddress ClientIPAddress { get; set; }

        public IPAddress YourIPAddress { get; set; }

        public IPAddress NextServerIPAddress { get; set; }

        public IPAddress RelayAgentIPAddress { get; set; }

        public byte[] ClientHardwareAddress { get; set; }

        public string ServerHostName { get; set; }

        public string BootFileName { get; set; }

        public List<IDHCPOption> Options { get; set; }

        /// <summary>
        /// Convenience property to easily get or set the messagetype option
        /// </summary>
        public TDHCPMessageType MessageType
        {
            get
            {
                DHCPOptionMessageType messageTypeDHCPOption = (DHCPOptionMessageType)GetOption(TDHCPOption.MessageType);
                if(messageTypeDHCPOption != null)
                {
                    return messageTypeDHCPOption.MessageType;
                }
                else
                {
                    return TDHCPMessageType.Undefined;
                }
            }
            set
            {
                TDHCPMessageType currentMessageType = MessageType;
                if(currentMessageType != value)
                {
                    Options.Add(new DHCPOptionMessageType(value));
                }
            }
        }

        private static void RegisterOption(IDHCPOption option)
        {
            s_optionsTemplates[(int)option.OptionType] = option;
        }

        static DHCPMessage()
        {
            s_optionsTemplates = new IDHCPOption[256];
            for(int t = 1; t < 255; t++)
            {
                s_optionsTemplates[t] = new DHCPOptionGeneric((TDHCPOption)t);
            }

            RegisterOption(new DHCPOptionFixedLength(TDHCPOption.Pad));
            RegisterOption(new DHCPOptionFixedLength(TDHCPOption.End));
            RegisterOption(new DHCPOptionHostName());
            RegisterOption(new DHCPOptionIPAddressLeaseTime());
            RegisterOption(new DHCPOptionServerIdentifier());
            RegisterOption(new DHCPOptionRequestedIPAddress());
            RegisterOption(new DHCPOptionOptionOverload());
            RegisterOption(new DHCPOptionTFTPServerName());
            RegisterOption(new DHCPOptionBootFileName());
            RegisterOption(new DHCPOptionMessageType());
            RegisterOption(new DHCPOptionMessage());
            RegisterOption(new DHCPOptionMaximumDHCPMessageSize());
            RegisterOption(new DHCPOptionParameterRequestList());
            RegisterOption(new DHCPOptionRenewalTimeValue());
            RegisterOption(new DHCPOptionRebindingTimeValue());
            RegisterOption(new DHCPOptionVendorClassIdentifier());
            RegisterOption(new DHCPOptionClientIdentifier());
            RegisterOption(new DHCPOptionFullyQualifiedDomainName());
            RegisterOption(new DHCPOptionSubnetMask());
            RegisterOption(new DHCPOptionRouter());
            RegisterOption(new DHCPOptionDomainNameServer());
            RegisterOption(new DHCPOptionNetworkTimeProtocolServers());
#if RELAYAGENTINFORMATION
            RegisterOption(new DHCPOptionRelayAgentInformation());
#endif
        }

        public DHCPMessage()
        {
            HardwareType = THardwareType.Ethernet;
            ClientIPAddress = IPAddress.Any;
            YourIPAddress = IPAddress.Any;
            NextServerIPAddress = IPAddress.Any;
            RelayAgentIPAddress = IPAddress.Any;
            ClientHardwareAddress = [];
            ServerHostName = "";
            BootFileName = "";
            Options = [];
        }

        public IDHCPOption GetOption(TDHCPOption optionType)
        {
            return Options.Find(delegate (IDHCPOption v) { return v.OptionType == optionType; });
        }

        public bool IsRequestedParameter(TDHCPOption optionType)
        {
            DHCPOptionParameterRequestList dhcpOptionParameterRequestList = (DHCPOptionParameterRequestList)GetOption(TDHCPOption.ParameterRequestList);
            return (dhcpOptionParameterRequestList != null && dhcpOptionParameterRequestList.RequestList.Contains(optionType));
        }

        private DHCPMessage(Stream s) : this()
        {
            Opcode = (TOpcode)s.ReadByte();
            HardwareType = (THardwareType)s.ReadByte();
            ClientHardwareAddress = new byte[s.ReadByte()];
            Hops = (byte)s.ReadByte();
            XID = ParseHelper.ReadUInt32(s);
            Secs = ParseHelper.ReadUInt16(s);
            BroadCast = ((ParseHelper.ReadUInt16(s) & 0x8000) == 0x8000);
            ClientIPAddress = ParseHelper.ReadIPAddress(s);
            YourIPAddress = ParseHelper.ReadIPAddress(s);
            NextServerIPAddress = ParseHelper.ReadIPAddress(s);
            RelayAgentIPAddress = ParseHelper.ReadIPAddress(s);
            s.Read(ClientHardwareAddress, 0, ClientHardwareAddress.Length);
            for(int t = ClientHardwareAddress.Length; t < 16; t++) s.ReadByte();

            byte[] serverHostNameBuffer = new byte[64];
            s.Read(serverHostNameBuffer, 0, serverHostNameBuffer.Length);

            byte[] bootFileNameBuffer = new byte[128];
            s.Read(bootFileNameBuffer, 0, bootFileNameBuffer.Length);

            // read options magic cookie
            if(s.ReadByte() != 99) throw new IOException();
            if(s.ReadByte() != 130) throw new IOException();
            if(s.ReadByte() != 83) throw new IOException();
            if(s.ReadByte() != 99) throw new IOException();

            byte[] optionsBuffer = new byte[s.Length - s.Position];
            s.Read(optionsBuffer, 0, optionsBuffer.Length);

            byte overload = ScanOverload(new MemoryStream(optionsBuffer));

            switch(overload)
            {
                default:
                    ServerHostName = ParseHelper.ReadZString(new MemoryStream(serverHostNameBuffer));
                    BootFileName = ParseHelper.ReadZString(new MemoryStream(bootFileNameBuffer));
                    Options = ReadOptions(optionsBuffer, new byte[0], new byte[0]);
                    break;

                case 1:
                    ServerHostName = ParseHelper.ReadZString(new MemoryStream(serverHostNameBuffer));
                    Options = ReadOptions(optionsBuffer, bootFileNameBuffer, new byte[0]);
                    break;

                case 2:
                    BootFileName = ParseHelper.ReadZString(new MemoryStream(bootFileNameBuffer));
                    Options = ReadOptions(optionsBuffer, serverHostNameBuffer, new byte[0]);
                    break;

                case 3:
                    Options = ReadOptions(optionsBuffer, bootFileNameBuffer, serverHostNameBuffer);
                    break;
            }
        }

        private static List<IDHCPOption> ReadOptions(byte[] buffer1, byte[] buffer2, byte[] buffer3)
        {
            var result = new List<IDHCPOption>();
            ReadOptions(result, new MemoryStream(buffer1, true), new MemoryStream(buffer2, true), new MemoryStream(buffer3, true));
            ReadOptions(result, new MemoryStream(buffer2, true), new MemoryStream(buffer3, true));
            ReadOptions(result, new MemoryStream(buffer3, true));
            return result;
        }

        private static void CopyBytes(Stream source, Stream target, int length)
        {
            byte[] buffer = new byte[length];
            source.Read(buffer, 0, length);
            target.Write(buffer, 0, length);
        }

        private static void ReadOptions(List<IDHCPOption> options, MemoryStream s, params MemoryStream[] spillovers)
        {
            while(true)
            {
                int code = s.ReadByte();
                if(code == -1 || code == 255) break;
                else if(code == 0) continue;
                else
                {
                    MemoryStream concatStream = new MemoryStream();
                    int len = s.ReadByte();
                    if(len == -1) break;
                    CopyBytes(s, concatStream, len);
                    AppendOverflow(code, s, concatStream);
                    foreach(MemoryStream spillOver in spillovers)
                    {
                        AppendOverflow(code, spillOver, concatStream);
                    }
                    concatStream.Position = 0;
                    options.Add(s_optionsTemplates[code].FromStream(concatStream));
                }
            }
        }

        private static void AppendOverflow(int code, MemoryStream source, MemoryStream target)
        {
            long initPosition = source.Position;
            try
            {
                while(true)
                {
                    int c = source.ReadByte();
                    if(c == -1 || c == 255) break;
                    else if(c == 0) continue;
                    else
                    {
                        int l = source.ReadByte();
                        if(l == -1) break;

                        if(c == code)
                        {
                            long startPosition = source.Position - 2;
                            CopyBytes(source, target, l);
                            source.Position = startPosition;
                            for(int t = 0; t < (l + 2); t++)
                            {
                                source.WriteByte(0);
                            }
                        }
                        else
                        {
                            source.Seek(l, SeekOrigin.Current);
                        }
                    }
                }
            }
            finally
            {
                source.Position = initPosition;
            }
        }

        /// <summary>
        /// Locate the overload option value in the passed stream.
        /// </summary>
        /// <param name="s"></param>
        /// <returns>Returns the overload option value, or 0 if it wasn't found</returns>
        private static byte ScanOverload(Stream s)
        {
            byte result = 0;

            while(true)
            {
                int code = s.ReadByte();
                if(code == -1 || code == 255) break;
                else if(code == 0) continue;
                else if(code == 52)
                {
                    if(s.ReadByte() != 1) throw new IOException("Invalid length of DHCP option 'Option Overload'");
                    result = (byte)s.ReadByte();
                }
                else
                {
                    int l = s.ReadByte();
                    if(l == -1) break;
                    s.Position += l;
                }
            }
            return result;
        }

        public static DHCPMessage FromStream(Stream s)
        {
            return new DHCPMessage(s);
        }

        public void ToStream(Stream s, int minimumPacketSize)
        {
            s.WriteByte((byte)Opcode);
            s.WriteByte((byte)HardwareType);
            s.WriteByte((byte)ClientHardwareAddress.Length);
            s.WriteByte((byte)Hops);
            ParseHelper.WriteUInt32(s, XID);
            ParseHelper.WriteUInt16(s, Secs);
            ParseHelper.WriteUInt16(s, BroadCast ? (ushort)0x8000 : (ushort)0x0);
            ParseHelper.WriteIPAddress(s, ClientIPAddress);
            ParseHelper.WriteIPAddress(s, YourIPAddress);
            ParseHelper.WriteIPAddress(s, NextServerIPAddress);
            ParseHelper.WriteIPAddress(s, RelayAgentIPAddress);
            s.Write(ClientHardwareAddress, 0, ClientHardwareAddress.Length);
            for(int t = ClientHardwareAddress.Length; t < 16; t++) s.WriteByte(0);
            ParseHelper.WriteZString(s, ServerHostName, 64);  // BOOTP legacy
            ParseHelper.WriteZString(s, BootFileName, 128);   // BOOTP legacy
            s.Write(new byte[] { 99, 130, 83, 99 }, 0, 4);  // options magic cookie

            // write all options except RelayAgentInformation
            foreach(var option in Options.Where(x => x.OptionType != TDHCPOption.RelayAgentInformation))
            {
                var optionStream = new MemoryStream();
                option.ToStream(optionStream);
                s.WriteByte((byte)option.OptionType);
                s.WriteByte((byte)optionStream.Length);
                optionStream.Position = 0;
                CopyBytes(optionStream, s, (int)optionStream.Length);
            }

#if RELAYAGENTINFORMATION
            // RelayAgentInformation should be the last option before the end according to RFC 3046
            foreach (var option in _options.Where(x => x.OptionType == TDHCPOption.RelayAgentInformation))
            {
                var optionStream = new MemoryStream();
                option.ToStream(optionStream);
                s.WriteByte((byte)option.OptionType);
                s.WriteByte((byte)optionStream.Length);
                optionStream.Position = 0;
                CopyBytes(optionStream, s, (int)optionStream.Length);
            }
#endif
            // write end option
            s.WriteByte(255);
            s.Flush();

            while(s.Length < minimumPacketSize)
            {
                s.WriteByte(0);
            }

            s.Flush();
        }

        public override string ToString()
        {
            StringBuilder sb = new();

            sb.AppendFormat("Opcode (op)                    : {0}\r\n", Opcode);
            sb.AppendFormat("HardwareType (htype)           : {0}\r\n", HardwareType);
            sb.AppendFormat("Hops                           : {0}\r\n", Hops);
            sb.AppendFormat("XID                            : {0}\r\n", XID);
            sb.AppendFormat("Secs                           : {0}\r\n", Secs);
            sb.AppendFormat("BroadCast (flags)              : {0}\r\n", BroadCast);
            sb.AppendFormat("ClientIPAddress (ciaddr)       : {0}\r\n", ClientIPAddress);
            sb.AppendFormat("YourIPAddress (yiaddr)         : {0}\r\n", YourIPAddress);
            sb.AppendFormat("NextServerIPAddress (siaddr)   : {0}\r\n", NextServerIPAddress);
            sb.AppendFormat("RelayAgentIPAddress (giaddr)   : {0}\r\n", RelayAgentIPAddress);
            sb.AppendFormat("ClientHardwareAddress (chaddr) : {0}\r\n", Utils.BytesToHexString(ClientHardwareAddress, "-"));
            sb.AppendFormat("ServerHostName (sname)         : {0}\r\n", ServerHostName);
            sb.AppendFormat("BootFileName (file)            : {0}\r\n", BootFileName);

            foreach(IDHCPOption option in Options)
            {
                sb.AppendFormat("Option                         : {0}\r\n", option.ToString());
            }

            return sb.ToString();
        }
    }
}
