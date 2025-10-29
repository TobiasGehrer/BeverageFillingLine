using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;

namespace OpcMqttBridge
{
    public class OpcUaClient
    {
        private readonly string _endpointUrl;
        private readonly string _applicationName;
        private Session? _session;
        private readonly List<string> _nodeIds;

        public OpcUaClient(string endpointUrl, string applicationName)
        {
            _endpointUrl = endpointUrl;
            _applicationName = applicationName;
            _nodeIds = InitializeNodeIds();
        }

        private List<string> InitializeNodeIds()
        {
            return new List<string>
            {
                // Machine Identification
                "ns=2;s=MachineName",
                "ns=2;s=MachineSerialNumber",
                "ns=2;s=Plant",
                "ns=2;s=ProductionSegment",
                "ns=2;s=ProductionLine",
        
                // Production Order
                "ns=2;s=ProductionOrder",
                "ns=2;s=Article",
                "ns=2;s=Quantity",
                "ns=2;s=CurrentLotNumber",
                "ns=2;s=ExpirationDate",
        
                // Target Values
                "ns=2;s=TargetFillVolume",
                "ns=2;s=TargetLineSpeed",
                "ns=2;s=TargetProductTemperature",
                "ns=2;s=TargetCO2Pressure",
                "ns=2;s=TargetCapTorque",
                "ns=2;s=TargetCycleTime",
        
                // Actual Values
                "ns=2;s=ActualFillVolume",
                "ns=2;s=ActualLineSpeed",
                "ns=2;s=ActualProductTemperature",
                "ns=2;s=ActualCO2Pressure",
                "ns=2;s=ActualCapTorque",
                "ns=2;s=ActualCycleTime",
                "ns=2;s=FillAccuracyDeviation",
        
                // System Status
                "ns=2;s=MachineStatus",
                "ns=2;s=CurrentStation",
                "ns=2;s=ProductLevelTank",
                "ns=2;s=CleaningCycleStatus",
                "ns=2;s=QualityCheckWeight",
                "ns=2;s=QualityCheckLevel",
        
                // Counters
                "ns=2;s=GoodBottles",
                "ns=2;s=BadBottlesVolume",
                "ns=2;s=BadBottlesWeight",
                "ns=2;s=BadBottlesCap",
                "ns=2;s=BadBottlesOther",
                "ns=2;s=TotalBadBottles",
                "ns=2;s=TotalBottles",
                "ns=2;s=GoodBottlesOrder",
                "ns=2;s=BadBottlesOrder",
                "ns=2;s=TotalBottlesOrder",
                "ns=2;s=ProductionOrderProgress",
        
                // Alarms
                "ns=2;s=ActiveAlarms",
                "ns=2;s=AlarmCount"
            };
        }

        public async Task ConnectAsync()
        {
            var application = new ApplicationInstance
            {
                ApplicationName = _applicationName,
                ApplicationType = ApplicationType.Client,
            };

            var config = new ApplicationConfiguration
            {
                ApplicationName = _applicationName,
                ApplicationType = ApplicationType.Client,
                ApplicationUri = $"urn:{System.Net.Dns.GetHostName()}:{_applicationName}",
                ProductUri = $"uri:{_applicationName}",

                ServerConfiguration = new ServerConfiguration
                {
                    MaxSessionCount = 100,
                    MaxSessionTimeout = 3600000,
                    MinSessionTimeout = 10000,
                },

                SecurityConfiguration = new SecurityConfiguration
                {
                    ApplicationCertificate = new CertificateIdentifier
                    {
                        StoreType = "Directory",
                        StorePath = Path.Combine(Directory.GetCurrentDirectory(), "pki", "own")
                    },
                    TrustedPeerCertificates = new CertificateTrustList
                    {
                        StoreType = "Directory",
                        StorePath = Path.Combine(Directory.GetCurrentDirectory(), "pki", "trusted")
                    },
                    TrustedIssuerCertificates = new CertificateTrustList
                    {
                        StoreType = "Directory",
                        StorePath = Path.Combine(Directory.GetCurrentDirectory(), "pki", "issuer")
                    },
                    RejectedCertificateStore = new CertificateTrustList
                    {
                        StoreType = "Directory",
                        StorePath = Path.Combine(Directory.GetCurrentDirectory(), "pki", "rejected")
                    },
                    AutoAcceptUntrustedCertificates = true,
                    AddAppCertToTrustedStore = true,
                    RejectSHA1SignedCertificates = false,
                    MinimumCertificateKeySize = 1024,
                },

                TransportQuotas = new TransportQuotas
                {
                    OperationTimeout = 600000,
                    MaxStringLength = 1048576,
                    MaxByteStringLength = 1048576,
                    MaxArrayLength = 65535,
                    MaxMessageSize = 4194304,
                    MaxBufferSize = 65535,
                    ChannelLifetime = 300000,
                    SecurityTokenLifetime = 3600000
                },

                ClientConfiguration = new ClientConfiguration
                {
                    DefaultSessionTimeout = 60000,
                    MinSubscriptionLifetime = 10000,
                },

                TraceConfiguration = new TraceConfiguration
                {
                    OutputFilePath = "%CommonApplicationData%\\OPC Foundation\\Logs\\OpcUaClient.log",
                    DeleteOnLoad = true,
                    TraceMasks = 0
                },

                DisableHiResClock = false
            };

            // Validate the configuration
            await config.Validate(ApplicationType.Client);

            application.ApplicationConfiguration = config;

            var endpoint = CoreClientUtils.SelectEndpoint(config, _endpointUrl, false);
            var endpointConfiguration = EndpointConfiguration.Create(config);
            var configuredEndpoint = new ConfiguredEndpoint(null, endpoint, endpointConfiguration);

            _session = await Session.Create(
                config,
                configuredEndpoint,
                false,
                _applicationName,
                60000,
                new UserIdentity(new AnonymousIdentityToken()),
                null
            );
        }

        public async Task<Dictionary<string, object>> ReadAllVariablesAsync()
        {
            var result = new Dictionary<string, object>();

            foreach (var nodeId in _nodeIds)
            {
                try
                {
                    var value = _session!.ReadValue(nodeId);
                    var variableName = nodeId.Split('=').Last();
                    result[variableName] = value.Value ?? "null";
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error reading {nodeId}: {ex.Message}");
                }
            }

            return await Task.FromResult(result);
        }

        public async Task DisconnectAsync()
        {
            try
            {
                if (_session != null && _session.Connected)
                {
                    _session.Close();
                    _session.Dispose();
                    _session = null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Disconnect error: {ex.Message}");
            }
            await Task.CompletedTask;
        }

    }
}
