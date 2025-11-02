using Opc.Ua;
using Opc.Ua.Configuration;

namespace OpcServer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Beverage Filling Line Server...");

            try
            {
                var application = new ApplicationInstance
                {
                    ApplicationName = "Beverage Filling Line Server",
                    ApplicationType = ApplicationType.Server
                };

                var config = new ApplicationConfiguration
                {
                    ApplicationName = "Beverage Filling Line Server",
                    ApplicationUri = "urn:localhost:BeverageFillingLineServer",
                    ApplicationType = ApplicationType.Server,

                    ServerConfiguration = new ServerConfiguration
                    {
                        BaseAddresses = new StringCollection { "opc.tcp://0.0.0.0:4840" },
                        SecurityPolicies = new ServerSecurityPolicyCollection
                        {
                            new ServerSecurityPolicy
                            {
                                SecurityMode = MessageSecurityMode.None,
                                SecurityPolicyUri = SecurityPolicies.None
                            }
                        },
                        UserTokenPolicies = new UserTokenPolicyCollection
                        {
                            new UserTokenPolicy(UserTokenType.Anonymous)
                        },
                        MaxSessionCount = 10,
                        MaxSessionTimeout = 30000,
                        MinRequestThreadCount = 1,
                        MaxRequestThreadCount = 1,
                        MaxQueuedRequestCount = 10
                    },

                    SecurityConfiguration = new SecurityConfiguration
                    {
                        AutoAcceptUntrustedCertificates = true,
                        RejectSHA1SignedCertificates = false,
                        RejectUnknownRevocationStatus = false,
                        SuppressNonceValidationErrors = true,
                        AddAppCertToTrustedStore = true,
                        ApplicationCertificate = new CertificateIdentifier
                        {
                            StoreType = "Directory",
                            StorePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "OPC Foundation", "pki", "own"),
                            SubjectName = "CN=Beverage Filling Line Server, O=FluidFill Systems, DC=localhost"
                        },
                        TrustedIssuerCertificates = new CertificateTrustList
                        {
                            StoreType = "Directory",
                            StorePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "OPC Foundation", "pki", "issuer")
                        },
                        TrustedPeerCertificates = new CertificateTrustList
                        {
                            StoreType = "Directory",
                            StorePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "OPC Foundation", "pki", "trusted")
                        },
                        RejectedCertificateStore = new CertificateTrustList
                        {
                            StoreType = "Directory",
                            StorePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "OPC Foundation", "pki", "rejected")
                        }
                    },

                    TransportQuotas = new TransportQuotas
                    {
                        OperationTimeout = 10000,
                        MaxStringLength = 1048576,
                        MaxByteStringLength = 1048576,
                        MaxArrayLength = 65535,
                        MaxMessageSize = 4194304,
                        MaxBufferSize = 65535,
                        ChannelLifetime = 300000,
                        SecurityTokenLifetime = 3600000
                    }
                };

                application.ApplicationConfiguration = config;

                // Ensure certificate directories exist
                string pkiPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "OPC Foundation", "pki");
                Directory.CreateDirectory(Path.Combine(pkiPath, "own"));
                Directory.CreateDirectory(Path.Combine(pkiPath, "trusted"));
                Directory.CreateDirectory(Path.Combine(pkiPath, "issuer"));
                Directory.CreateDirectory(Path.Combine(pkiPath, "rejected"));

                // Handle certificates
                try
                {
                    bool certOK = await application.CheckApplicationInstanceCertificates(false, 0);

                    if (!certOK)
                    {
                        await application.CheckApplicationInstanceCertificates(true, 2048);
                    }
                }
                catch (Exception certEx)
                {
                    Console.WriteLine($"Certificate issue: {certEx.Message}");
                    await application.CheckApplicationInstanceCertificates(true, 2048);
                }

                var server = new BeverageFillingLineServer();
                await application.Start(server);

                Console.WriteLine("Server started at: opc.tcp://localhost:4840");
                Console.WriteLine("Server running... Press Ctrl+C to stop.");

                // Create a TaskCompletionSource to keep the app running
                var tcs = new TaskCompletionSource<bool>();
                Console.CancelKeyPress += (sender, e) => { e.Cancel = true; tcs.SetResult(true); };

                // Wait until cancellation
                await tcs.Task;

                server.Stop();
                Console.WriteLine("Server stopped.");               
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.ReadKey();
            }
        }
    }
}