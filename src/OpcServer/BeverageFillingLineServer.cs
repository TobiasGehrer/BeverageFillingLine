using Opc.Ua;
using Opc.Ua.Server;

namespace OpcServer
{
    public class BeverageFillingLineServer : StandardServer
    {
        private BeverageFillingLineMachine _machine;
        private Timer _simulationTimer;

        public BeverageFillingLineServer()
        {
            _machine = new BeverageFillingLineMachine();
        }

        protected override ServerProperties LoadServerProperties()
        {
            return new ServerProperties
            {
                ManufacturerName = "FluidFill Systems",
                ProductName = "Beverage Filling Line Server",
                ProductUri = "urn:FluidFill:BeverageServer",
                SoftwareVersion = "1.0.0",
                BuildNumber = "1",
                BuildDate = DateTime.Now
            };
        }

        protected override MasterNodeManager CreateMasterNodeManager(IServerInternal server, ApplicationConfiguration configuration)
        {
            Console.WriteLine("Creating master node manager...");

            try
            {
                var nodeManager = new BeverageFillingLineNodeManager(server, configuration, _machine);
                var masterNodeManager = new MasterNodeManager(server, configuration, null, nodeManager);

                // Start simulation timer - 1 second interval
                _simulationTimer = new Timer(UpdateSimulation, null, 1000, 1000);

                Console.WriteLine("Node manager created successfully");

                return masterNodeManager;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating node manager: {ex.Message}");
                throw;
            }
        }

        private void UpdateSimulation(object state)
        {
            try
            {
                _machine.UpdateSimulation();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Simulation error: {ex.Message}");
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _simulationTimer?.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}