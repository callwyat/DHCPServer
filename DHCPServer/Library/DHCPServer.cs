using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

namespace GitHub.JPMikkers.DHCP
{
    /// <summary>
    /// A Dynamic Host Configuration Protocol Server
    /// </summary>
    public class DHCPServer : IDHCPServer
    {
        /// <summary>
        /// Max Retires for client info to disk
        /// </summary>
        private const int ClientInformationWriteRetries = 10;

        /// <summary>
        /// Logger
        /// </summary>
        private readonly ILogger<DHCPServer> _logger;

        /// <summary>
        /// An object to lock on for thread synchronization
        /// </summary>
        private readonly object _sync = new();

        private UDPSocket? _socket;

        /// <summary>
        /// The path to a file to save information to between reboots
        /// </summary>
        private readonly string _clientInfoPath;
        private readonly Dictionary<DHCPClient, DHCPClient> _clients = [];
        private Timer? _timer;
        private bool _active = false;
        private readonly AutoPumpQueue<int> _updateClientInfoQueue;
        private readonly Random _random = new();

        #region IDHCPServer Members

        /// <inheritdoc/>
        public event EventHandler<DHCPStopEventArgs> OnStatusChange = delegate (object sender, DHCPStopEventArgs args) { };

        /// <inheritdoc/>
        public IPEndPoint EndPoint { get; set; } = new(IPAddress.Loopback, 67);

        /// <inheritdoc/>
        public IPAddress SubnetMask { get; set; } = IPAddress.Any;

        /// <inheritdoc/>
        public IPAddress PoolStart { get; set; } = IPAddress.Any;

        /// <inheritdoc/>
        public IPAddress PoolEnd { get; set; } = IPAddress.Broadcast;

        /// <inheritdoc/>
        public TimeSpan OfferExpirationTime { get; set; } = TimeSpan.FromSeconds(30.0);

        private TimeSpan _leaseTime = TimeSpan.FromDays(1);

        /// <inheritdoc/>
        public TimeSpan LeaseTime
        {
            get => _leaseTime;
            set => _leaseTime = Utils.SanitizeTimeSpan(value);
        }

        private int _minimumPacketSize = 576;

        /// <summary>
        /// The smallest packet size
        /// </summary>
        /// <remarks>
        /// Value must be larger the 312
        /// </remarks>
        public int MinimumPacketSize
        {
            get => _minimumPacketSize;
            set => _minimumPacketSize = Math.Max(value, 312);
        }

        /// <inheritdoc/>
        public string HostName { get; }

        /// <inheritdoc/>
        public IReadOnlyList<DHCPClient> Clients
        {
            get
            {
                lock(_clients)
                {
                    return _clients.Values.ToList().AsReadOnly();
                }
            }
        }

        /// <inheritdoc/>
        public bool Active
        {
            get
            {
                lock(_sync)
                {
                    return _active;
                }
            }
        }

        /// <inheritdoc/>
        public List<OptionItem> Options { get; set; } = [];

        /// <summary>
        /// Allows for external options to be attached to a message
        /// </summary>
        public List<IDHCPMessageInterceptor> Interceptors { get; set; } = [];

        /// <inheritdoc/>
        public List<ReservationItem> Reservations { get; set; } = [];

        private void OnUpdateClientInfo(AutoPumpQueue<int> sender, int data)
        {
            if(Active)
            {
                try
                {
                    DHCPClientInformation clientInformation = new();

                    foreach(DHCPClient client in Clients)
                    {
                        clientInformation.Clients.Add(client);
                    }

                    for(int t = 0; t < ClientInformationWriteRetries; t++)
                    {
                        try
                        {
                            clientInformation.Write(_clientInfoPath);
                            break;
                        }
                        catch
                        {
                        }

                        if(t < ClientInformationWriteRetries)
                        {
                            Thread.Sleep(_random.Next(500, 1000));
                        }
                        else
                        {
                            _logger.LogDebug("Could not update client information, data might be stale");
                        }
                    }
                }
                catch(Exception ex)
                {
                    _logger.LogWarning(ex, $"Exception in OnUpdateClientInfo");
                }
            }
        }

        /// <summary>
        /// Creates a new Dynamic Host Configuration Protocol Server
        /// </summary>
        /// <param name="clientInfoPath">The to save client info to between sessions</param>
        /// <param name="logger">A logger</param>
        public DHCPServer(string clientInfoPath, ILogger<DHCPServer> logger)
        {
            _logger = logger;
            _updateClientInfoQueue = new AutoPumpQueue<int>(OnUpdateClientInfo);
            _clientInfoPath = clientInfoPath;

            HostName = Environment.MachineName;
        }

        public void Start()
        {
            lock(_sync)
            {
                if(!_active)
                {
                    if(File.Exists(_clientInfoPath))
                    {
                        _logger.LogTrace("Reading existing client info from: {Path}", _clientInfoPath);
                        try
                        {
                            DHCPClientInformation clientInformation = DHCPClientInformation.Read(_clientInfoPath);

                            foreach(DHCPClient client in clientInformation.Clients
                                .Where(c => c.State != DHCPClient.TState.Offered)   // Forget offered clients.
                                .Where(c => IsIPAddressInPoolRange(c.IPAddress)))   // Forget clients no longer in ip range.
                            {
                                _clients.Add(client, client);
                            }
                        }
                        catch(Exception ex)
                        {
                            _logger.LogDebug(ex, "Failed to read the existing client info");
                        }
                    }
                    else
                    {
                        _logger.LogTrace("Failed to find client info at: {Path}", _clientInfoPath);
                    }


                    try
                    {
                        _logger.LogInformation("Starting DHCP server '{EndPoint}'", EndPoint);
                        _active = true;
                        _socket = new UDPSocket(EndPoint, 2048, true, 10, OnReceive, OnStop);
                        _timer = new Timer(new TimerCallback(OnTimer), null, TimeSpan.FromSeconds(1.0), TimeSpan.FromSeconds(1.0));
                        _logger.LogDebug("DHCP Server start succeeded");
                    }
                    catch(Exception ex)
                    {
                        _logger.LogError(ex, "DHCP Server start failed");
                        _active = false;
                        throw;
                    }
                }
                else
                {
                    _logger.LogWarning("An attempt to start the server was made while the server was already active");
                }
            }

            HandleStatusChange();
        }

        public void Stop()
        {
            Stop(null);
            _timer?.Dispose();
        }

        #endregion

        #region Dispose pattern

        ~DHCPServer()
        {
            try
            {
                Dispose(false);
            }
            catch
            {
                // never let any exception escape the finalizer, or else your process will be killed.
            }
        }

        protected void Dispose(bool disposing)
        {
            if(disposing)
            {
                Stop();
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        #endregion

        private void HandleStatusChange(Exception? reason = null)
        {
            _updateClientInfoQueue.Enqueue(0);
            OnStatusChange(this, reason is null ?
                DHCPStopEventArgs.Empty :
                new DHCPStopEventArgs()
                {
                    Reason = reason
                });
        }

        private void Stop(Exception? reason)
        {
            lock(_sync)
            {
                if(_active)
                {
                    _logger.LogInformation("Stopping DHCP server '{EndPoint}'", EndPoint);
                    _active = false;

                    _socket?.Dispose();
                    _logger.LogDebug("Stopped");

                    HandleStatusChange(reason);
                }
            }
        }

        private void OnTimer(object state)
        {
            bool modified = false;

            lock(_clients)
            {
                List<DHCPClient> clientsToRemove = new List<DHCPClient>();
                foreach(DHCPClient client in _clients.Keys)
                {
                    if(client.State == DHCPClient.TState.Offered && (DateTime.Now - client.OfferedTime) > OfferExpirationTime)
                    {
                        clientsToRemove.Add(client);
                    }
                    else if(client.State == DHCPClient.TState.Assigned && (DateTime.Now > client.LeaseEndTime))
                    {
                        // lease expired. remove client
                        clientsToRemove.Add(client);
                    }
                }

                foreach(DHCPClient client in clientsToRemove)
                {
                    _clients.Remove(client);
                    modified = true;
                }
            }

            if(modified)
            {
                HandleStatusChange();
            }
        }

        private void RemoveClient(DHCPClient client)
        {
            lock(_clients)
            {
                if(_clients.Remove(client))
                {
                    _logger.LogDebug("Removed client '{Client}' from client table", client);
                }
            }
        }

        private void SendMessage(DHCPMessage msg, IPEndPoint endPoint)
        {
            if(_socket is null)
            {
                throw new InvalidOperationException("Server must be started before a message can be sent");
            }

            if(_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("==== Sending response to {EndPoint} ====", endPoint);
                _logger.LogTrace(Utils.PrefixLines(msg.ToString(), "s->c "));
            }

            MemoryStream m = new();
            msg.ToStream(m, _minimumPacketSize);
            _socket.Send(endPoint, new ArraySegment<byte>(m.ToArray()));
        }

        private void AppendDefaultOptions(DHCPMessage sourceMsg, DHCPMessage targetMsg)
        {
            if(_socket is not null)
            {
                targetMsg.Options.Add(new DHCPOptionServerIdentifier(((IPEndPoint)_socket.LocalEndPoint).Address));
            }

            if(sourceMsg.IsRequestedParameter(TDHCPOption.SubnetMask))
            {
                targetMsg.Options.Add(new DHCPOptionSubnetMask(SubnetMask));
            }
        }

        private void AppendConfiguredOptions(DHCPMessage sourceMsg, DHCPMessage targetMsg)
        {
            foreach(OptionItem optionItem in Options)
            {
                if(optionItem.Mode == OptionMode.Force || sourceMsg.IsRequestedParameter(optionItem.Option.OptionType))
                {
                    if(targetMsg.GetOption(optionItem.Option.OptionType) == null)
                    {
                        targetMsg.Options.Add(optionItem.Option);
                    }
                }
            }

            foreach(IDHCPMessageInterceptor interceptor in Interceptors)
            {
                interceptor.Apply(sourceMsg, targetMsg);
            }
        }

        private void SendOFFER(DHCPMessage sourceMsg, IPAddress offeredAddress, TimeSpan leaseTime)
        {
            //Field      DHCPOFFER            
            //-----      ---------            
            //'op'       BOOTREPLY            
            //'htype'    (From "Assigned Numbers" RFC)
            //'hlen'     (Hardware address length in octets)
            //'hops'     0                    
            //'xid'      'xid' from client DHCPDISCOVER message              
            //'secs'     0                    
            //'ciaddr'   0                    
            //'yiaddr'   IP address offered to client            
            //'siaddr'   IP address of next bootstrap server     
            //'flags'    'flags' from client DHCPDISCOVER message              
            //'giaddr'   'giaddr' from client DHCPDISCOVER message              
            //'chaddr'   'chaddr' from client DHCPDISCOVER message              
            //'sname'    Server host name or options           
            //'file'     Client boot file name or options      
            //'options'  options              
            DHCPMessage response = new DHCPMessage();
            response.Opcode = DHCPMessage.TOpcode.BootReply;
            response.HardwareType = sourceMsg.HardwareType;
            response.Hops = 0;
            response.XID = sourceMsg.XID;
            response.Secs = 0;
            response.ClientIPAddress = IPAddress.Any;
            response.YourIPAddress = offeredAddress;
            response.NextServerIPAddress = IPAddress.Any;
            response.BroadCast = sourceMsg.BroadCast;
            response.RelayAgentIPAddress = sourceMsg.RelayAgentIPAddress;
            response.ClientHardwareAddress = sourceMsg.ClientHardwareAddress;
            response.MessageType = TDHCPMessageType.OFFER;

            //Option                    DHCPOFFER    
            //------                    ---------    
            //Requested IP address      MUST NOT     : ok
            //IP address lease time     MUST         : ok                                               
            //Use 'file'/'sname' fields MAY          
            //DHCP message type         DHCPOFFER    : ok
            //Parameter request list    MUST NOT     : ok
            //Message                   SHOULD       
            //Client identifier         MUST NOT     : ok
            //Vendor class identifier   MAY          
            //Server identifier         MUST         : ok
            //Maximum message size      MUST NOT     : ok
            //All others                MAY          

            response.Options.Add(new DHCPOptionIPAddressLeaseTime(leaseTime));
            AppendDefaultOptions(sourceMsg, response);
            AppendConfiguredOptions(sourceMsg, response);
            SendOfferOrAck(sourceMsg, response);
        }

        private void SendNAK(DHCPMessage sourceMsg)
        {
            //Field      DHCPNAK
            //-----      -------
            //'op'       BOOTREPLY
            //'htype'    (From "Assigned Numbers" RFC)
            //'hlen'     (Hardware address length in octets)
            //'hops'     0
            //'xid'      'xid' from client DHCPREQUEST message
            //'secs'     0
            //'ciaddr'   0
            //'yiaddr'   0
            //'siaddr'   0
            //'flags'    'flags' from client DHCPREQUEST message
            //'giaddr'   'giaddr' from client DHCPREQUEST message
            //'chaddr'   'chaddr' from client DHCPREQUEST message
            //'sname'    (unused)
            //'file'     (unused)
            //'options'  
            DHCPMessage response = new DHCPMessage();
            response.Opcode = DHCPMessage.TOpcode.BootReply;
            response.HardwareType = sourceMsg.HardwareType;
            response.Hops = 0;
            response.XID = sourceMsg.XID;
            response.Secs = 0;
            response.ClientIPAddress = IPAddress.Any;
            response.YourIPAddress = IPAddress.Any;
            response.NextServerIPAddress = IPAddress.Any;
            response.BroadCast = sourceMsg.BroadCast;
            response.RelayAgentIPAddress = sourceMsg.RelayAgentIPAddress;
            response.ClientHardwareAddress = sourceMsg.ClientHardwareAddress;
            response.MessageType = TDHCPMessageType.NAK;
            response.Options.Add(new DHCPOptionServerIdentifier(((IPEndPoint)_socket!.LocalEndPoint).Address));
            if(sourceMsg.IsRequestedParameter(TDHCPOption.SubnetMask))
            {
                response.Options.Add(new DHCPOptionSubnetMask(SubnetMask));
            }

            if(!sourceMsg.RelayAgentIPAddress.Equals(IPAddress.Any))
            {
                // If the 'giaddr' field in a DHCP message from a client is non-zero,
                // the server sends any return messages to the 'DHCP server' port on the
                // BOOTP relay agent whose address appears in 'giaddr'.
                SendMessage(response, new IPEndPoint(sourceMsg.RelayAgentIPAddress, 67));
            }
            else
            {
                // In all cases, when 'giaddr' is zero, the server broadcasts any DHCPNAK
                // messages to 0xffffffff.
                SendMessage(response, new IPEndPoint(IPAddress.Broadcast, 68));
            }
        }

        private void SendACK(DHCPMessage sourceMsg, IPAddress assignedAddress, TimeSpan leaseTime)
        {
            //Field      DHCPACK             
            //-----      -------             
            //'op'       BOOTREPLY           
            //'htype'    (From "Assigned Numbers" RFC)
            //'hlen'     (Hardware address length in octets)
            //'hops'     0                   
            //'xid'      'xid' from client DHCPREQUEST message             
            //'secs'     0                   
            //'ciaddr'   'ciaddr' from DHCPREQUEST or 0
            //'yiaddr'   IP address assigned to client
            //'siaddr'   IP address of next bootstrap server
            //'flags'    'flags' from client DHCPREQUEST message             
            //'giaddr'   'giaddr' from client DHCPREQUEST message             
            //'chaddr'   'chaddr' from client DHCPREQUEST message             
            //'sname'    Server host name or options
            //'file'     Client boot file name or options
            //'options'  options
            DHCPMessage response = new DHCPMessage();
            response.Opcode = DHCPMessage.TOpcode.BootReply;
            response.HardwareType = sourceMsg.HardwareType;
            response.Hops = 0;
            response.XID = sourceMsg.XID;
            response.Secs = 0;
            response.ClientIPAddress = sourceMsg.ClientIPAddress;
            response.YourIPAddress = assignedAddress;
            response.NextServerIPAddress = IPAddress.Any;
            response.BroadCast = sourceMsg.BroadCast;
            response.RelayAgentIPAddress = sourceMsg.RelayAgentIPAddress;
            response.ClientHardwareAddress = sourceMsg.ClientHardwareAddress;
            response.MessageType = TDHCPMessageType.ACK;

            //Option                    DHCPACK            
            //------                    -------            
            //Requested IP address      MUST NOT           : ok
            //IP address lease time     MUST (DHCPREQUEST) : ok
            //Use 'file'/'sname' fields MAY                
            //DHCP message type         DHCPACK            : ok
            //Parameter request list    MUST NOT           : ok
            //Message                   SHOULD             
            //Client identifier         MUST NOT           : ok
            //Vendor class identifier   MAY                
            //Server identifier         MUST               : ok
            //Maximum message size      MUST NOT           : ok  
            //All others                MAY                

            response.Options.Add(new DHCPOptionIPAddressLeaseTime(leaseTime));
            AppendDefaultOptions(sourceMsg, response);
            AppendConfiguredOptions(sourceMsg, response);
            SendOfferOrAck(sourceMsg, response);
        }

        private void SendINFORMACK(DHCPMessage sourceMsg)
        {
            // The server responds to a DHCPINFORM message by sending a DHCPACK
            // message directly to the address given in the 'ciaddr' field of the
            // DHCPINFORM message.  The server MUST NOT send a lease expiration time
            // to the client and SHOULD NOT fill in 'yiaddr'.  The server includes
            // other parameters in the DHCPACK message as defined in section 4.3.1.

            //Field      DHCPACK             
            //-----      -------             
            //'op'       BOOTREPLY           
            //'htype'    (From "Assigned Numbers" RFC)
            //'hlen'     (Hardware address length in octets)
            //'hops'     0                   
            //'xid'      'xid' from client DHCPREQUEST message             
            //'secs'     0                   
            //'ciaddr'   'ciaddr' from DHCPREQUEST or 0
            //'yiaddr'   IP address assigned to client
            //'siaddr'   IP address of next bootstrap server
            //'flags'    'flags' from client DHCPREQUEST message             
            //'giaddr'   'giaddr' from client DHCPREQUEST message             
            //'chaddr'   'chaddr' from client DHCPREQUEST message             
            //'sname'    Server host name or options
            //'file'     Client boot file name or options
            //'options'  options
            DHCPMessage response = new DHCPMessage();
            response.Opcode = DHCPMessage.TOpcode.BootReply;
            response.HardwareType = sourceMsg.HardwareType;
            response.Hops = 0;
            response.XID = sourceMsg.XID;
            response.Secs = 0;
            response.ClientIPAddress = sourceMsg.ClientIPAddress;
            response.YourIPAddress = IPAddress.Any;
            response.NextServerIPAddress = IPAddress.Any;
            response.BroadCast = sourceMsg.BroadCast;
            response.RelayAgentIPAddress = sourceMsg.RelayAgentIPAddress;
            response.ClientHardwareAddress = sourceMsg.ClientHardwareAddress;
            response.MessageType = TDHCPMessageType.ACK;

            //Option                    DHCPACK            
            //------                    -------            
            //Requested IP address      MUST NOT              : ok
            //IP address lease time     MUST NOT (DHCPINFORM) : ok
            //Use 'file'/'sname' fields MAY                
            //DHCP message type         DHCPACK               : ok
            //Parameter request list    MUST NOT              : ok
            //Message                   SHOULD             
            //Client identifier         MUST NOT              : ok
            //Vendor class identifier   MAY                
            //Server identifier         MUST                  : ok
            //Maximum message size      MUST NOT              : ok
            //All others                MAY                

            AppendDefaultOptions(sourceMsg, response);
            AppendConfiguredOptions(sourceMsg, response);
            SendMessage(response, new IPEndPoint(sourceMsg.ClientIPAddress, 68));
        }

        private void SendOfferOrAck(DHCPMessage request, DHCPMessage response)
        {
            // RFC2131.txt, 4.1, paragraph 4

            // DHCP messages broadcast by a client prior to that client obtaining
            // its IP address must have the source address field in the IP header
            // set to 0.

            if(!request.RelayAgentIPAddress.Equals(IPAddress.Any))
            {
                // If the 'giaddr' field in a DHCP message from a client is non-zero,
                // the server sends any return messages to the 'DHCP server' port on the
                // BOOTP relay agent whose address appears in 'giaddr'.
                SendMessage(response, new IPEndPoint(request.RelayAgentIPAddress, 67));
            }
            else
            {
                if(!request.ClientIPAddress.Equals(IPAddress.Any))
                {
                    // If the 'giaddr' field is zero and the 'ciaddr' field is nonzero, then the server
                    // unicasts DHCPOFFER and DHCPACK messages to the address in 'ciaddr'.
                    SendMessage(response, new IPEndPoint(request.ClientIPAddress, 68));
                }
                else
                {
                    // If 'giaddr' is zero and 'ciaddr' is zero, and the broadcast bit is
                    // set, then the server broadcasts DHCPOFFER and DHCPACK messages to
                    // 0xffffffff. If the broadcast bit is not set and 'giaddr' is zero and
                    // 'ciaddr' is zero, then the server unicasts DHCPOFFER and DHCPACK
                    // messages to the client's hardware address and 'yiaddr' address.  
                    SendMessage(response, new IPEndPoint(IPAddress.Broadcast, 68));
                }
            }
        }

        private bool ServerIdentifierPrecondition(DHCPMessage msg)
        {
            bool result = false;
            DHCPOptionServerIdentifier dhcpOptionServerIdentifier = (DHCPOptionServerIdentifier)msg.GetOption(TDHCPOption.ServerIdentifier);

            if(dhcpOptionServerIdentifier != null)
            {
                if(dhcpOptionServerIdentifier.IPAddress.Equals(EndPoint.Address))
                {
                    result = true;
                }
                else
                {
                    _logger.LogDebug("Client sent message with non-matching server identifier '{IPAddress}' -> ignored", dhcpOptionServerIdentifier.IPAddress);
                }
            }
            else
            {
                _logger.LogDebug("Client sent message without filling required ServerIdentifier option -> ignored");
            }
            return result;
        }

        private bool IsIPAddressInRange(IPAddress address, IPAddress start, IPAddress end)
        {
            var adr32 = Utils.IPAddressToUInt32(address);
            return adr32 >= Utils.IPAddressToUInt32(SanitizeHostRange(start)) && adr32 <= Utils.IPAddressToUInt32(SanitizeHostRange(end));
        }

        /// <summary>
        /// Checks whether the given IP address falls within the known pool ranges.
        /// </summary>
        /// <param name="address">IP address to check</param>
        /// <returns>true when the ip address matches one of the known pool ranges</returns>
        private bool IsIPAddressInPoolRange(IPAddress address)
        {
            return IsIPAddressInRange(address, PoolStart, PoolEnd) || Reservations.Any(r => IsIPAddressInRange(address, r.PoolStart, r.PoolEnd));
        }

        private bool IPAddressIsInSubnet(IPAddress address)
        {
            return ((Utils.IPAddressToUInt32(address) & Utils.IPAddressToUInt32(SubnetMask)) == (Utils.IPAddressToUInt32(EndPoint.Address) & Utils.IPAddressToUInt32(SubnetMask)));
        }

        private bool IPAddressIsFree(IPAddress address, bool reuseReleased)
        {
            if(!IPAddressIsInSubnet(address)) return false;
            if(address.Equals(EndPoint.Address)) return false;
            foreach(DHCPClient client in _clients.Keys)
            {
                if(client.IPAddress.Equals(address))
                {
                    if(reuseReleased && client.State == DHCPClient.TState.Released)
                    {
                        client.IPAddress = IPAddress.Any;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        private IPAddress SanitizeHostRange(IPAddress startend)
        {
            return Utils.UInt32ToIPAddress(
                (Utils.IPAddressToUInt32(EndPoint.Address) & Utils.IPAddressToUInt32(SubnetMask)) |
                (Utils.IPAddressToUInt32(startend) & ~Utils.IPAddressToUInt32(SubnetMask))
            );
        }

        private IPAddress AllocateIPAddress(DHCPMessage dhcpMessage)
        {
            DHCPOptionRequestedIPAddress dhcpOptionRequestedIPAddress = (DHCPOptionRequestedIPAddress)dhcpMessage.GetOption(TDHCPOption.RequestedIPAddress);

            ReservationItem? reservation = Reservations.FirstOrDefault(x => x.Match(dhcpMessage));

            if(reservation != null)
            {
                // the client matches a reservation.. find the first free address in the reservation block
                for(UInt32 host = Utils.IPAddressToUInt32(SanitizeHostRange(reservation.PoolStart)); host <= Utils.IPAddressToUInt32(SanitizeHostRange(reservation.PoolEnd)); host++)
                {
                    IPAddress testIPAddress = Utils.UInt32ToIPAddress(host);
                    // I don't see the point of avoiding released addresses for reservations (yet)
                    if(IPAddressIsFree(testIPAddress, true))
                    {
                        return testIPAddress;
                    }
                    else if(reservation.Preempt)
                    {
                        // if Preempt is true, return the first address of the reservation range. Preempt should ONLY ever be used if the range is a single address, and you're 100% sure you'll 
                        // _always_ have just a single device in your network that matches the reservation MAC or name.
                        return testIPAddress;
                    }
                }
            }

            if(dhcpOptionRequestedIPAddress != null)
            {
                // there is a requested IP address. Is it in our subnet and free?
                if(IPAddressIsFree(dhcpOptionRequestedIPAddress.IPAddress, true))
                {
                    // yes, the requested address is ok
                    return dhcpOptionRequestedIPAddress.IPAddress;
                }
            }

            // first try to find a free address without reusing released ones
            for(UInt32 host = Utils.IPAddressToUInt32(SanitizeHostRange(PoolStart)); host <= Utils.IPAddressToUInt32(SanitizeHostRange(PoolEnd)); host++)
            {
                IPAddress testIPAddress = Utils.UInt32ToIPAddress(host);
                if(IPAddressIsFree(testIPAddress, false))
                {
                    return testIPAddress;
                }
            }

            // nothing found.. now start allocating released ones as well
            for(UInt32 host = Utils.IPAddressToUInt32(SanitizeHostRange(PoolStart)); host <= Utils.IPAddressToUInt32(SanitizeHostRange(PoolEnd)); host++)
            {
                IPAddress testIPAddress = Utils.UInt32ToIPAddress(host);
                if(IPAddressIsFree(testIPAddress, true))
                {
                    return testIPAddress;
                }
            }

            // still nothing: report failure
            return IPAddress.Any;
        }

        private void OfferClient(DHCPMessage dhcpMessage, DHCPClient client)
        {
            lock(_clients)
            {
                client.State = DHCPClient.TState.Offered;
                client.OfferedTime = DateTime.Now;
                if(!_clients.ContainsKey(client)) _clients.Add(client, client);
                SendOFFER(dhcpMessage, client.IPAddress, _leaseTime);
            }
        }

        private void OnReceive(UDPSocket sender, IPEndPoint endPoint, ArraySegment<byte> data)
        {
            try
            {
                _logger.LogTrace("Incoming packet - parsing DHCP Message");

                // translate array segment into a DHCPMessage
                DHCPMessage dhcpMessage = DHCPMessage.FromStream(new MemoryStream(data.Array, data.Offset, data.Count, false, false));

                if(_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace(Utils.PrefixLines(dhcpMessage.ToString(), "c->s "));
                }

                // only react to messages from client to server. Ignore other types.
                if(dhcpMessage.Opcode == DHCPMessage.TOpcode.BootRequest)
                {
                    DHCPClient client = DHCPClient.CreateFromMessage(dhcpMessage);
                    _logger.LogTrace("Client {Client} sent {MessageType}", client, dhcpMessage.MessageType);

                    switch(dhcpMessage.MessageType)
                    {
                        // DHCPDISCOVER - client to server
                        // broadcast to locate available servers
                        case TDHCPMessageType.DISCOVER:
                            lock(_clients)
                            {
                                // is it a known client?
                                if(_clients.TryGetValue(client, out DHCPClient knownClient))
                                {
                                    _logger.LogTrace("Client is known, in state {State}", knownClient.State);

                                    if(knownClient.State == DHCPClient.TState.Offered || knownClient.State == DHCPClient.TState.Assigned)
                                    {
                                        _logger.LogDebug("Client sent DISCOVER but we already offered, or assigned -> repeat offer with known address");
                                        OfferClient(dhcpMessage, knownClient);
                                    }
                                    else
                                    {
                                        _logger.LogDebug("Client is known but released");
                                        // client is known but released or dropped. Use the old address or allocate a new one
                                        if(knownClient.IPAddress.Equals(IPAddress.Any))
                                        {
                                            knownClient.IPAddress = AllocateIPAddress(dhcpMessage);
                                            if(!knownClient.IPAddress.Equals(IPAddress.Any))
                                            {
                                                OfferClient(dhcpMessage, knownClient);
                                            }
                                            else
                                            {
                                                _logger.LogError("No more free addresses. Don't respond to discover");
                                            }
                                        }
                                        else
                                        {
                                            OfferClient(dhcpMessage, knownClient);
                                        }
                                    }
                                }
                                else
                                {
                                    _logger.LogTrace("Client is not known yet");
                                    // client is not known yet.
                                    // allocate new address, add client to client table in Offered state
                                    client.IPAddress = AllocateIPAddress(dhcpMessage);
                                    // allocation ok ?
                                    if(!client.IPAddress.Equals(IPAddress.Any))
                                    {
                                        OfferClient(dhcpMessage, client);
                                    }
                                    else
                                    {
                                        _logger.LogError("No more free addresses. Don't respond to discover");
                                    }
                                }
                            }
                            break;

                        // DHCPREQUEST - client to server
                        // Client message to servers either 
                        // (a) requesting offered parameters from one server and implicitly declining offers from all others.
                        // (b) confirming correctness of previously allocated address after e.g. system reboot, or
                        // (c) extending the lease on a particular network address
                        case TDHCPMessageType.REQUEST:
                            lock(_clients)
                            {
                                // is it a known client?
                                DHCPClient? knownClient = _clients.ContainsKey(client) ? _clients[client] : null;

                                // is there a server identifier?
                                DHCPOptionServerIdentifier dhcpOptionServerIdentifier = (DHCPOptionServerIdentifier)dhcpMessage.GetOption(TDHCPOption.ServerIdentifier);
                                DHCPOptionRequestedIPAddress dhcpOptionRequestedIPAddress = (DHCPOptionRequestedIPAddress)dhcpMessage.GetOption(TDHCPOption.RequestedIPAddress);

                                if(dhcpOptionServerIdentifier != null)
                                {
                                    // there is a server identifier: the message is in response to a DHCPOFFER
                                    if(dhcpOptionServerIdentifier.IPAddress.Equals(EndPoint.Address))
                                    {
                                        // it's a response to OUR offer.
                                        // but did we actually offer one?
                                        if(knownClient != null && knownClient.State == DHCPClient.TState.Offered)
                                        {
                                            // yes.
                                            // the requested IP address MUST be filled in with the offered address
                                            if(dhcpOptionRequestedIPAddress != null)
                                            {
                                                if(knownClient.IPAddress.Equals(dhcpOptionRequestedIPAddress.IPAddress))
                                                {
                                                    _logger.LogTrace("Client request matches offered address -> ACK");
                                                    knownClient.State = DHCPClient.TState.Assigned;
                                                    knownClient.LeaseStartTime = DateTime.Now;
                                                    knownClient.LeaseDuration = _leaseTime;
                                                    SendACK(dhcpMessage, knownClient.IPAddress, knownClient.LeaseDuration);
                                                }
                                                else
                                                {
                                                    _logger.LogDebug("Client sent request for IP address '{RequestedIPAddress}', but it does not match the offered address '{KnownIPAddress}' -> NAK", dhcpOptionRequestedIPAddress.IPAddress, knownClient.IPAddress);
                                                    SendNAK(dhcpMessage);
                                                    RemoveClient(knownClient);
                                                }
                                            }
                                            else
                                            {
                                                _logger.LogDebug("Client sent request without filling the RequestedIPAddress option -> NAK");
                                                SendNAK(dhcpMessage);
                                                RemoveClient(knownClient);
                                            }
                                        }
                                        else
                                        {
                                            // we don't have an outstanding offer!
                                            _logger.LogDebug("Client requested IP address from this server, but we didn't offer any. -> NAK");
                                            SendNAK(dhcpMessage);
                                        }
                                    }
                                    else
                                    {
                                        _logger.LogDebug("Client requests IP address that was offered by another DHCP server at '{IPAddress}' -> drop offer", dhcpOptionServerIdentifier.IPAddress);
                                        // it's a response to another DHCP server.
                                        // if we sent an OFFER to this client earlier, remove it.
                                        if(knownClient != null)
                                        {
                                            RemoveClient(knownClient);
                                        }
                                    }
                                }
                                else
                                {
                                    // no server identifier: the message is a request to verify or extend an existing lease
                                    // Received REQUEST without server identifier, client is INIT-REBOOT, RENEWING or REBINDING

                                    _logger.LogDebug("Received REQUEST without server identifier, client state is INIT-REBOOT, RENEWING or REBINDING");

                                    if(!dhcpMessage.ClientIPAddress.Equals(IPAddress.Any))
                                    {
                                        _logger.LogTrace("REQUEST client IP is filled in -> client state is RENEWING or REBINDING");

                                        // see : http://www.tcpipguide.com/free/t_DHCPLeaseRenewalandRebindingProcesses-2.htm

                                        if(knownClient != null &&
                                            knownClient.State == DHCPClient.TState.Assigned &&
                                            knownClient.IPAddress.Equals(dhcpMessage.ClientIPAddress))
                                        {
                                            // known, assigned, and IP address matches administration. ACK
                                            knownClient.LeaseStartTime = DateTime.Now;
                                            knownClient.LeaseDuration = _leaseTime;
                                            SendACK(dhcpMessage, dhcpMessage.ClientIPAddress, knownClient.LeaseDuration);
                                        }
                                        else
                                        {
                                            // not known, or known but in some other state. Just dump the old one.
                                            if(knownClient != null) RemoveClient(knownClient);

                                            // check if client IP address is marked free
                                            if(IPAddressIsFree(dhcpMessage.ClientIPAddress, false))
                                            {
                                                // it's free. send ACK
                                                client.IPAddress = dhcpMessage.ClientIPAddress;
                                                client.State = DHCPClient.TState.Assigned;
                                                client.LeaseStartTime = DateTime.Now;
                                                client.LeaseDuration = _leaseTime;
                                                _clients.Add(client, client);
                                                SendACK(dhcpMessage, dhcpMessage.ClientIPAddress, client.LeaseDuration);
                                            }
                                            else
                                            {
                                                _logger.LogDebug("Renewing client IP address already in use. Oops..");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        _logger.LogDebug("REQUEST client IP is empty -> client state is INIT-REBOOT");

                                        if(dhcpOptionRequestedIPAddress != null)
                                        {
                                            if(knownClient != null &&
                                                knownClient.State == DHCPClient.TState.Assigned)
                                            {
                                                if(knownClient.IPAddress.Equals(dhcpOptionRequestedIPAddress.IPAddress))
                                                {
                                                    _logger.LogTrace("Client request matches cached address -> ACK");
                                                    // known, assigned, and IP address matches administration. ACK
                                                    knownClient.LeaseStartTime = DateTime.Now;
                                                    knownClient.LeaseDuration = _leaseTime;
                                                    SendACK(dhcpMessage, dhcpOptionRequestedIPAddress.IPAddress, knownClient.LeaseDuration);
                                                }
                                                else
                                                {
                                                    _logger.LogDebug("Client sent request for IP address '{RequestedIPAddress}', but it does not match cached address '{KnownIPAddress}' -> NAK", dhcpOptionRequestedIPAddress.IPAddress, knownClient.IPAddress);
                                                    SendNAK(dhcpMessage);
                                                    RemoveClient(knownClient);
                                                }
                                            }
                                            else
                                            {
                                                // client not known, or known but in some other state.
                                                // send NAK so client will drop to INIT state where it can acquire a new lease.
                                                // see also: http://tcpipguide.com/free/t_DHCPGeneralOperationandClientFiniteStateMachine.htm
                                                _logger.LogDebug("Client attempted INIT-REBOOT REQUEST but server has no lease for this client -> NAK");
                                                SendNAK(dhcpMessage);
                                                if(knownClient != null) RemoveClient(knownClient);
                                            }
                                        }
                                        else
                                        {
                                            _logger.LogDebug("Client sent apparent INIT-REBOOT REQUEST but with an empty 'RequestedIPAddress' option. Oops..");
                                        }
                                    }
                                }
                            }
                            break;

                        case TDHCPMessageType.DECLINE:
                            // If the server receives a DHCPDECLINE message, the client has
                            // discovered through some other means that the suggested network
                            // address is already in use.  The server MUST mark the network address
                            // as not available and SHOULD notify the local system administrator of
                            // a possible configuration problem.
                            lock(_clients)
                            {
                                if(ServerIdentifierPrecondition(dhcpMessage))
                                {
                                    // is it a known client?
                                    if(_clients.TryGetValue(client, out DHCPClient knownClient))
                                    {
                                        _logger.LogTrace("Found client in client table, removing.");
                                        RemoveClient(client);

                                        /*
                                            // the network address that should be marked as not available MUST be 
                                            // specified in the RequestedIPAddress option.                                        
                                            DHCPOptionRequestedIPAddress dhcpOptionRequestedIPAddress = (DHCPOptionRequestedIPAddress)dhcpMessage.GetOption(TDHCPOption.RequestedIPAddress);
                                            if(dhcpOptionRequestedIPAddress!=null)
                                            {
                                                if(dhcpOptionRequestedIPAddress.IPAddress.Equals(knownClient.IPAddress))
                                                {
                                                    // TBD add IP address to exclusion list. or something.
                                                }
                                            }
                                         */
                                    }
                                    else
                                    {
                                        _logger.LogTrace("Client not found in client table -> decline ignored.");
                                    }
                                }
                            }
                            break;

                        case TDHCPMessageType.RELEASE:
                            // relinguishing network address and cancelling remaining lease.
                            // Upon receipt of a DHCPRELEASE message, the server marks the network
                            // address as not allocated.  The server SHOULD retain a record of the
                            // client's initialization parameters for possible reuse in response to
                            // subsequent requests from the client.
                            lock(_clients)
                            {
                                if(ServerIdentifierPrecondition(dhcpMessage))
                                {
                                    // is it a known client?
                                    if(_clients.TryGetValue(client, out DHCPClient knownClient)/* && knownClient.State == DHCPClient.TState.Assigned */ )
                                    {
                                        if(dhcpMessage.ClientIPAddress.Equals(knownClient.IPAddress))
                                        {
                                            _logger.LogTrace("Found client in client table, marking as released");
                                            knownClient.State = DHCPClient.TState.Released;
                                        }
                                        else
                                        {
                                            _logger.LogTrace("IP address in RELEASE doesn't match known client address. Mark this client as released with unknown IP");
                                            knownClient.IPAddress = IPAddress.Any;
                                            knownClient.State = DHCPClient.TState.Released;
                                        }
                                    }
                                    else
                                    {
                                        _logger.LogTrace("Client not found in client table, release ignored.");
                                    }
                                }
                            }
                            break;

                        // DHCPINFORM - client to server
                        // client asking for local configuration parameters, client already has externally configured
                        // network address.
                        case TDHCPMessageType.INFORM:
                            // The server responds to a DHCPINFORM message by sending a DHCPACK
                            // message directly to the address given in the 'ciaddr' field of the
                            // DHCPINFORM message.  The server MUST NOT send a lease expiration time
                            // to the client and SHOULD NOT fill in 'yiaddr'.  The server includes
                            // other parameters in the DHCPACK message as defined in section 4.3.1.
                            SendINFORMACK(dhcpMessage);
                            break;

                        default:
                            _logger.LogTrace("Invalid message from client, ignored");
                            break;
                    }

                    HandleStatusChange();
                }
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Error OnReceive");
            }
        }

        private void OnStop(UDPSocket sender, Exception reason)
        {
            Stop(reason);
        }
    }
}
