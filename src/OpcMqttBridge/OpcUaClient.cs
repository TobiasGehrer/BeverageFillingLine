using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System.Linq;

namespace OpcMqttBridge
{
    public class OpcUaClient
    {
        private readonly string _endpointUrl;
        private readonly string _applicationName;
        private Session? _session;
        private Subscription? _subscription;
        private readonly List<string> _nodeIds;
        private bool _isReconnecting = false;

        // Event fired when monitored items change
        public event EventHandler<ValueChangedEventArgs>? ValueChanged;

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

                DisableHiResClock = false
            };

            // Validate the configuration
            await config.Validate(ApplicationType.Client);

            application.ApplicationConfiguration = config;

            var endpoint = CoreClientUtils.SelectEndpoint(config, _endpointUrl, false);
            var endpointConfiguration = EndpointConfiguration.Create(config);
            var configuredEndpoint = new ConfiguredEndpoint(null, endpoint, endpointConfiguration);

            // Create session with keep-alive handler
            _session = await Session.Create(
                config,
                configuredEndpoint,
                false,
                _applicationName,
                60000,
                new UserIdentity(new AnonymousIdentityToken()),
                null);

            // Set up session keep-alive for reconnection
            _session.KeepAlive += Session_KeepAlive;

            Console.WriteLine("OPC UA session created successfully");
        }

        public async Task<Dictionary<string, object>> ReadAllVariablesAsync()
        {
            if (_session == null || !_session.Connected)
                throw new InvalidOperationException("Not connected to OPC UA server");

            var readValueIds = new ReadValueIdCollection();
            foreach (var nodeId in _nodeIds)
            {
                readValueIds.Add(new ReadValueId
                {
                    NodeId = new NodeId(nodeId),
                    AttributeId = Attributes.Value
                });
            }

            var response = _session.Read(null, 0, TimestampsToReturn.Neither, readValueIds, out var results, out _);

            var data = new Dictionary<string, object>();
            for (int i = 0; i < _nodeIds.Count && i < results.Count; i++)
            {
                if (StatusCode.IsGood(results[i].StatusCode))
                {
                    var nodeId = _nodeIds[i].Split(';').Last().Replace("s=", "");
                    data[nodeId] = results[i].Value;
                }
            }

            return data;
        }

        public async Task CreateSubscriptionAsync(int publishingInterval = 1000)
        {
            if (_session == null || !_session.Connected)
                throw new InvalidOperationException("Not connected to OPC UA server");

            // Create subscription
            _subscription = new Subscription(_session.DefaultSubscription)
            {
                PublishingEnabled = true,
                PublishingInterval = publishingInterval,
                KeepAliveCount = 10,
                LifetimeCount = 100,
                MaxNotificationsPerPublish = 1000,
                Priority = 100
            };

            // Add monitored items for all node IDs
            foreach (var nodeId in _nodeIds)
            {
                var monitoredItem = new MonitoredItem(_subscription.DefaultItem)
                {
                    StartNodeId = new NodeId(nodeId),
                    AttributeId = Attributes.Value,
                    DisplayName = nodeId,
                    SamplingInterval = publishingInterval,
                    QueueSize = 1,
                    DiscardOldest = true
                };

                // Set up notification handler
                monitoredItem.Notification += OnMonitoredItemNotification;

                _subscription.AddItem(monitoredItem);
            }

            // Create the subscription on the server
            await Task.Run(() => _session.AddSubscription(_subscription));
            _subscription.Create();

            Console.WriteLine($"Subscription created with {_subscription.MonitoredItemCount} monitored items");
            Console.WriteLine($"Publishing interval: {publishingInterval}ms");
        }

        private void OnMonitoredItemNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                // Extract node name from the StartNodeId
                var nodeId = item.StartNodeId.ToString();
                var nodeName = nodeId.Split(';').Last().Replace("s=", "");

                if (e.NotificationValue is MonitoredItemNotification notification)
                {
                    // Fire the ValueChanged event with node name and value
                    ValueChanged?.Invoke(this, new ValueChangedEventArgs
                    {
                        NodeName = nodeName,
                        Value = notification.Value.Value,
                        Timestamp = notification.Value.SourceTimestamp
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing notification: {ex.Message}");
            }
        }

        private async void Session_KeepAlive(ISession session, KeepAliveEventArgs e)
        {
            if (e.Status != null && ServiceResult.IsNotGood(e.Status))
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Keep-alive failed: {e.Status}");

                if (!_isReconnecting && _session != null)
                {
                    _isReconnecting = true;
                    Console.WriteLine("Attempting to reconnect...");

                    try
                    {
                        await ReconnectAsync();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Reconnection failed: {ex.Message}");
                    }
                    finally
                    {
                        _isReconnecting = false;
                    }
                }
            }
        }

        private async Task ReconnectAsync()
        {
            try
            {
                if (_session != null)
                {
                    await _session.ReconnectAsync(CancellationToken.None);
                    Console.WriteLine("Reconnected successfully");

                    // Recreate subscription if it was lost
                    if (_subscription != null && !_subscription.Created)
                    {
                        _subscription.Create();
                        Console.WriteLine("Subscription recreated");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Reconnect error: {ex.Message}");
                throw;
            }
        }

        public bool IsConnected()
        {
            return _session != null && _session.Connected;
        }

        public async Task DisconnectAsync()
        {
            try
            {
                // Remove subscription
                if (_subscription != null && _session != null)
                {
                    _session.RemoveSubscription(_subscription);
                    _subscription.Delete(true);
                    _subscription.Dispose();
                    _subscription = null;
                }

                // Close session
                if (_session != null && _session.Connected)
                {
                    _session.KeepAlive -= Session_KeepAlive;
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

    public class ValueChangedEventArgs : EventArgs
    {
        public string NodeName { get; set; } = string.Empty;
        public object Value { get; set; } = null!;
        public DateTime Timestamp { get; set; }
    }
}