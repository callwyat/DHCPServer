
using System.CommandLine;
using System.Text.RegularExpressions;
using System.Linq;
using Microsoft.Extensions.Logging;
using GitHub.JPMikkers.DHCP;
using System.Net;

namespace Console
{
    public static class Program
    {
        public static int Main(string[] args)
        {
            Regex verboseMatch = new(@"^(-v{1,3}|--verbose)$");

            int verboseCount = args.Where(a => verboseMatch.IsMatch(a))
                .Sum(a => a.Count(c => c == 'v'));

            using ILoggerFactory logFactory = LoggerFactory.Create(b => b
                .AddConsole()
                .SetMinimumLevel((LogLevel)Math.Max((int)(LogLevel.Information - verboseCount), (int)LogLevel.Trace))
            );

            ILogger logger = logFactory.CreateLogger(nameof(Program));

            RootCommand root = [];

            Option<bool> verboseOption = new("--verbose", "Write more to the console");
            verboseOption.AddAlias("-v");
            root.AddGlobalOption(verboseOption);

            Option<string> hostAddress = new("--hostAddress", "The IPv4 Address to host the DHCP Server from");
            hostAddress.SetDefaultValue("127.0.0.1");

            hostAddress.AddValidator(a => 
            {
                string? value = a.GetValueOrDefault<string>();
                if (!IPAddress.TryParse(value, out _))
                {
                    a.ErrorMessage += "Invalid Host Address";
                }
            });

            root.Add(hostAddress);

            root.SetHandler(hostAddress => {

                string cachePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create),
                    "DHCPServer", "cache.xml");

                ManualResetEvent resetEvent = new(false);

                logger.LogTrace("Starting DHCPServer");
                using DHCPServer server = new(logFactory.CreateLogger<DHCPServer>(), cachePath, new DefaultUDPSocketFactory(logFactory.CreateLogger<DefaultUDPSocketFactory>()))
                {
                    EndPoint = new IPEndPoint(IPAddress.Parse(hostAddress), 67),
                    PoolStart = IPAddress.Parse("192.168.1.210"),
                    PoolEnd = IPAddress.Parse("192.168.1.250"),
                };

                // server.Options.Add(new DHCPOptionTFTPServerName(Environment.MachineName));
 
                server.Options.Add(new OptionItem(OptionMode.Default, new DHCPOptionBootFileName("menu.ipxe")));
                server.Options.Add(new OptionItem(OptionMode.Default, new DHCPOptionTFTPServerName("192.168.1.9")));

                server.OnStatusChange += (sender, e) =>
                {
                    if (!server.Active)
                    {
                        logger.LogTrace("DCHPServer Stopped");

                        resetEvent.Set();
                    }
                };

                server.Start();

                resetEvent.WaitOne();
            }, hostAddress);

            return root.Invoke(args);
        }
    }
}