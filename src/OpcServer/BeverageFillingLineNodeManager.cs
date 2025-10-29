using Opc.Ua;
using Opc.Ua.Server;

namespace OpcServer
{
    public class BeverageFillingLineNodeManager : CustomNodeManager2
    {
        private BeverageFillingLineMachine _machine;
        private Dictionary<string, BaseDataVariableState> _variables;
        private Timer _updateTimer;

        public BeverageFillingLineNodeManager(IServerInternal server, ApplicationConfiguration configuration, BeverageFillingLineMachine machine)
            : base(server, configuration, "urn:BeverageServer:")
        {
            _machine = machine;
            _variables = new Dictionary<string, BaseDataVariableState>();
            SetNamespaces("urn:BeverageServer:");
        }

        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            lock (Lock)
            {
                LoadPredefinedNodes(SystemContext, externalReferences);

                // Start updating OPC variables
                _updateTimer = new Timer(UpdateOpcVariables, null, 0, 3000);
            }
        }

        protected override NodeStateCollection LoadPredefinedNodes(ISystemContext context)
        {
            NodeStateCollection predefinedNodes = new NodeStateCollection();

            try
            {
                // Create root folder
                FolderState root = new FolderState(null)
                {
                    NodeId = new NodeId("BeverageFillingLine", NamespaceIndex),
                    BrowseName = new QualifiedName("BeverageFillingLine", NamespaceIndex),
                    DisplayName = new LocalizedText("Beverage Filling Line"),
                    TypeDefinitionId = ObjectTypeIds.FolderType
                };

                root.AddReference(ReferenceTypes.Organizes, true, ObjectIds.ObjectsFolder);
                AddPredefinedNode(context, root);
                predefinedNodes.Add(root);

                // Machine Identification
                CreateVariable(root, "MachineName", DataTypeIds.String, _machine.MachineName, predefinedNodes);
                CreateVariable(root, "MachineSerialNumber", DataTypeIds.String, _machine.MachineSerialNumber, predefinedNodes);
                CreateVariable(root, "Plant", DataTypeIds.String, _machine.Plant, predefinedNodes);
                CreateVariable(root, "ProductionSegment", DataTypeIds.String, _machine.ProductionSegment, predefinedNodes);
                CreateVariable(root, "ProductionLine", DataTypeIds.String, _machine.ProductionLine, predefinedNodes);

                // Production Order
                CreateVariable(root, "ProductionOrder", DataTypeIds.String, _machine.ProductionOrder, predefinedNodes);
                CreateVariable(root, "Article", DataTypeIds.String, _machine.Article, predefinedNodes);
                CreateVariable(root, "Quantity", DataTypeIds.UInt32, _machine.Quantity, predefinedNodes);
                CreateVariable(root, "CurrentLotNumber", DataTypeIds.String, _machine.CurrentLotNumber, predefinedNodes);
                CreateVariable(root, "ExpirationDate", DataTypeIds.DateTime, _machine.ExpirationDate, predefinedNodes);

                // Target Values
                CreateVariable(root, "TargetFillVolume", DataTypeIds.Double, _machine.TargetFillVolume, predefinedNodes);
                CreateVariable(root, "TargetLineSpeed", DataTypeIds.Double, _machine.TargetLineSpeed, predefinedNodes);
                CreateVariable(root, "TargetProductTemperature", DataTypeIds.Double, _machine.TargetProductTemperature, predefinedNodes);
                CreateVariable(root, "TargetCO2Pressure", DataTypeIds.Double, _machine.TargetCO2Pressure, predefinedNodes);
                CreateVariable(root, "TargetCapTorque", DataTypeIds.Double, _machine.TargetCapTorque, predefinedNodes);
                CreateVariable(root, "TargetCycleTime", DataTypeIds.Double, _machine.TargetCycleTime, predefinedNodes);

                // Actual Values
                CreateVariable(root, "ActualFillVolume", DataTypeIds.Double, _machine.ActualFillVolume, predefinedNodes);
                CreateVariable(root, "ActualLineSpeed", DataTypeIds.Double, _machine.ActualLineSpeed, predefinedNodes);
                CreateVariable(root, "ActualProductTemperature", DataTypeIds.Double, _machine.ActualProductTemperature, predefinedNodes);
                CreateVariable(root, "ActualCO2Pressure", DataTypeIds.Double, _machine.ActualCO2Pressure, predefinedNodes);
                CreateVariable(root, "ActualCapTorque", DataTypeIds.Double, _machine.ActualCapTorque, predefinedNodes);
                CreateVariable(root, "ActualCycleTime", DataTypeIds.Double, _machine.ActualCycleTime, predefinedNodes);
                CreateVariable(root, "FillAccuracyDeviation", DataTypeIds.Double, _machine.FillAccuracyDeviation, predefinedNodes);

                // System Status
                CreateVariable(root, "MachineStatus", DataTypeIds.String, _machine.MachineStatus, predefinedNodes);
                CreateVariable(root, "CurrentStation", DataTypeIds.String, _machine.CurrentStation, predefinedNodes);
                CreateVariable(root, "ProductLevelTank", DataTypeIds.Double, _machine.ProductLevelTank, predefinedNodes);
                CreateVariable(root, "CleaningCycleStatus", DataTypeIds.String, _machine.CleaningCycleStatus, predefinedNodes);
                CreateVariable(root, "QualityCheckWeight", DataTypeIds.String, _machine.QualityCheckWeight, predefinedNodes);
                CreateVariable(root, "QualityCheckLevel", DataTypeIds.String, _machine.QualityCheckLevel, predefinedNodes);

                // Counters - Global
                CreateVariable(root, "GoodBottles", DataTypeIds.UInt32, _machine.GoodBottles, predefinedNodes);
                CreateVariable(root, "BadBottlesVolume", DataTypeIds.UInt32, _machine.BadBottlesVolume, predefinedNodes);
                CreateVariable(root, "BadBottlesWeight", DataTypeIds.UInt32, _machine.BadBottlesWeight, predefinedNodes);
                CreateVariable(root, "BadBottlesCap", DataTypeIds.UInt32, _machine.BadBottlesCap, predefinedNodes);
                CreateVariable(root, "BadBottlesOther", DataTypeIds.UInt32, _machine.BadBottlesOther, predefinedNodes);
                CreateVariable(root, "TotalBadBottles", DataTypeIds.UInt32, _machine.TotalBadBottles, predefinedNodes);
                CreateVariable(root, "TotalBottles", DataTypeIds.UInt32, _machine.TotalBottles, predefinedNodes);

                // Counters - Current Order
                CreateVariable(root, "GoodBottlesOrder", DataTypeIds.UInt32, _machine.GoodBottlesOrder, predefinedNodes);
                CreateVariable(root, "BadBottlesOrder", DataTypeIds.UInt32, _machine.BadBottlesOrder, predefinedNodes);
                CreateVariable(root, "TotalBottlesOrder", DataTypeIds.UInt32, _machine.TotalBottlesOrder, predefinedNodes);
                CreateVariable(root, "ProductionOrderProgress", DataTypeIds.Double, _machine.ProductionOrderProgress, predefinedNodes);

                // Alarms
                CreateArrayVariable(root, "ActiveAlarms", DataTypeIds.String, _machine.ActiveAlarms.ToArray(), predefinedNodes);
                CreateVariable(root, "AlarmCount", DataTypeIds.UInt32, (uint)_machine.ActiveAlarms.Count, predefinedNodes);

                // Create Methods Folder
                FolderState methodsFolder = new FolderState(root)
                {
                    NodeId = new NodeId("Methods", NamespaceIndex),
                    BrowseName = new QualifiedName("Methods", NamespaceIndex),
                    DisplayName = new LocalizedText("Control Methods"),
                    TypeDefinitionId = ObjectTypeIds.FolderType
                };

                root.AddChild(methodsFolder);
                AddPredefinedNode(SystemContext, methodsFolder);
                predefinedNodes.Add(methodsFolder);

                // Create OPC UA Methods in Methods folder (11 methods from PDF)
                CreateMethod(methodsFolder, "StartMachine", "Starts the beverage filling machine", predefinedNodes);
                CreateMethod(methodsFolder, "StopMachine", "Stops the beverage filling machine", predefinedNodes);
                CreateMethod(methodsFolder, "EmergencyStop", "Emergency stop of the machine", predefinedNodes);
                CreateMethod(methodsFolder, "EnterMaintenanceMode", "Puts machine into maintenance mode", predefinedNodes);
                CreateMethod(methodsFolder, "StartCIPCycle", "Starts Clean-in-Place cycle", predefinedNodes);
                CreateMethod(methodsFolder, "StartSIPCycle", "Starts Sterilize-in-Place cycle", predefinedNodes);
                CreateMethod(methodsFolder, "ResetCounters", "Resets all production counters", predefinedNodes);
                CreateMethod(methodsFolder, "GenerateLotNumber", "Generates new lot number", predefinedNodes);

                // Production control methods with parameters
                CreateMethodWithParameters(methodsFolder, "AdjustFillVolume", "Adjusts target fill volume", predefinedNodes,
                    new List<Argument> {
                        new Argument("newFillVolume", DataTypeIds.Double, ValueRanks.Scalar, "New target fill volume in ml")
                    });

                CreateMethodWithParameters(methodsFolder, "LoadProductionOrder", "Loads new production order", predefinedNodes,
                    new List<Argument> {
                        new Argument("orderNumber", DataTypeIds.String, ValueRanks.Scalar, "Production order number"),
                        new Argument("article", DataTypeIds.String, ValueRanks.Scalar, "Article code"),
                        new Argument("quantity", DataTypeIds.UInt32, ValueRanks.Scalar, "Order quantity"),
                        new Argument("targetFillVolume", DataTypeIds.Double, ValueRanks.Scalar, "Target fill volume in ml"),
                        new Argument("targetLineSpeed", DataTypeIds.Double, ValueRanks.Scalar, "Target line speed BPM"),
                        new Argument("targetProductTemp", DataTypeIds.Double, ValueRanks.Scalar, "Target product temperature"),
                        new Argument("targetCO2Pressure", DataTypeIds.Double, ValueRanks.Scalar, "Target CO2 pressure"),
                        new Argument("targetCapTorque", DataTypeIds.Double, ValueRanks.Scalar, "Target cap torque"),
                        new Argument("targetCycleTime", DataTypeIds.Double, ValueRanks.Scalar, "Target cycle time")
                    });

                CreateMethodWithParameters(methodsFolder, "ChangeProduct", "Changes product specifications", predefinedNodes,
                    new List<Argument> {
                        new Argument("newArticle", DataTypeIds.String, ValueRanks.Scalar, "New article code"),
                        new Argument("newTargetFillVolume", DataTypeIds.Double, ValueRanks.Scalar, "New target fill volume"),
                        new Argument("newTargetProductTemp", DataTypeIds.Double, ValueRanks.Scalar, "New target product temperature"),
                        new Argument("newTargetCO2Pressure", DataTypeIds.Double, ValueRanks.Scalar, "New target CO2 pressure")
                    });

                return predefinedNodes;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating address space: {ex.Message}");
                throw;
            }
        }

        private void CreateMethodWithParameters(FolderState parent, string name, string description, NodeStateCollection predefinedNodes, List<Argument> inputArguments)
        {
            try
            {
                var method = new MethodState(parent)
                {
                    NodeId = new NodeId(name, NamespaceIndex),
                    BrowseName = new QualifiedName(name, NamespaceIndex),
                    DisplayName = new LocalizedText(name),
                    Description = new LocalizedText(description),
                    Executable = true,
                    UserExecutable = true
                };

                // Set input arguments
                method.InputArguments = new PropertyState<Argument[]>(method)
                {
                    NodeId = new NodeId(name + "_InputArguments", NamespaceIndex),
                    BrowseName = BrowseNames.InputArguments,
                    DisplayName = BrowseNames.InputArguments,
                    TypeDefinitionId = VariableTypeIds.PropertyType,
                    ReferenceTypeId = ReferenceTypes.HasProperty,
                    DataType = DataTypeIds.Argument,
                    ValueRank = ValueRanks.OneDimension,
                    Value = inputArguments.ToArray()
                };

                method.OnCallMethod = new GenericMethodCalledEventHandler(OnCallMethod);

                // Add reference from parent to method
                parent.AddChild(method);

                // Add to predefined nodes so it gets processed
                AddPredefinedNode(SystemContext, method);
                predefinedNodes.Add(method);

                // Add explicit reference for method visibility
                method.AddReference(ReferenceTypes.HasComponent, true, parent.NodeId);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to create method {name}: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private void CreateMethod(FolderState parent, string name, string description, NodeStateCollection predefinedNodes)
        {
            try
            {
                var method = new MethodState(parent)
                {
                    NodeId = new NodeId(name, NamespaceIndex),
                    BrowseName = new QualifiedName(name, NamespaceIndex),
                    DisplayName = new LocalizedText(name),
                    Description = new LocalizedText(description),
                    Executable = true,
                    UserExecutable = true
                };

                method.OnCallMethod = new GenericMethodCalledEventHandler(OnCallMethod);

                // Add reference from parent to method
                parent.AddChild(method);

                // Add to predefined nodes so it gets processed
                AddPredefinedNode(SystemContext, method);
                predefinedNodes.Add(method);

                // Add explicit reference for method visibility
                method.AddReference(ReferenceTypes.HasComponent, true, parent.NodeId);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to create method {name}: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private void CreateArrayVariable(FolderState parent, string name, NodeId dataType, object initialValue, NodeStateCollection predefinedNodes)
        {
            try
            {
                var variable = new BaseDataVariableState(parent)
                {
                    NodeId = new NodeId(name, NamespaceIndex),
                    BrowseName = new QualifiedName(name, NamespaceIndex),
                    DisplayName = new LocalizedText(name),
                    TypeDefinitionId = VariableTypeIds.BaseDataVariableType,
                    DataType = dataType,
                    ValueRank = ValueRanks.OneDimension,
                    ArrayDimensions = new ReadOnlyList<uint>(new List<uint> { 0 }),
                    AccessLevel = AccessLevels.CurrentRead,
                    UserAccessLevel = AccessLevels.CurrentRead,
                    Value = initialValue,
                    StatusCode = StatusCodes.Good,
                    Timestamp = DateTime.UtcNow
                };

                parent.AddChild(variable);
                predefinedNodes.Add(variable);
                _variables[name] = variable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create array variable {name}: {ex.Message}");
                throw;
            }
        }

        private void CreateVariable(FolderState parent, string name, NodeId dataType, object initialValue, NodeStateCollection predefinedNodes)
        {
            try
            {
                var variable = new BaseDataVariableState(parent)
                {
                    NodeId = new NodeId(name, NamespaceIndex),
                    BrowseName = new QualifiedName(name, NamespaceIndex),
                    DisplayName = new LocalizedText(name),
                    TypeDefinitionId = VariableTypeIds.BaseDataVariableType,
                    DataType = dataType,
                    ValueRank = ValueRanks.Scalar,
                    AccessLevel = AccessLevels.CurrentRead,
                    UserAccessLevel = AccessLevels.CurrentRead,
                    Value = initialValue,
                    StatusCode = StatusCodes.Good,
                    Timestamp = DateTime.UtcNow
                };

                parent.AddChild(variable);
                predefinedNodes.Add(variable);
                _variables[name] = variable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create variable {name}: {ex.Message}");
                throw;
            }
        }

        private void UpdateOpcVariables(object state)
        {
            try
            {
                lock (Lock)
                {
                    // Machine Identification (static, but update for consistency)
                    UpdateVariable("MachineName", _machine.MachineName);
                    UpdateVariable("MachineSerialNumber", _machine.MachineSerialNumber);
                    UpdateVariable("Plant", _machine.Plant);
                    UpdateVariable("ProductionSegment", _machine.ProductionSegment);
                    UpdateVariable("ProductionLine", _machine.ProductionLine);

                    // Production Order
                    UpdateVariable("ProductionOrder", _machine.ProductionOrder);
                    UpdateVariable("Article", _machine.Article);
                    UpdateVariable("Quantity", _machine.Quantity);
                    UpdateVariable("CurrentLotNumber", _machine.CurrentLotNumber);
                    UpdateVariable("ExpirationDate", _machine.ExpirationDate);

                    // Target Values
                    UpdateVariable("TargetFillVolume", _machine.TargetFillVolume);
                    UpdateVariable("TargetLineSpeed", _machine.TargetLineSpeed);
                    UpdateVariable("TargetProductTemperature", _machine.TargetProductTemperature);
                    UpdateVariable("TargetCO2Pressure", _machine.TargetCO2Pressure);
                    UpdateVariable("TargetCapTorque", _machine.TargetCapTorque);
                    UpdateVariable("TargetCycleTime", _machine.TargetCycleTime);

                    // Actual Values (dynamic)
                    UpdateVariable("ActualFillVolume", _machine.ActualFillVolume);
                    UpdateVariable("ActualLineSpeed", _machine.ActualLineSpeed);
                    UpdateVariable("ActualProductTemperature", _machine.ActualProductTemperature);
                    UpdateVariable("ActualCO2Pressure", _machine.ActualCO2Pressure);
                    UpdateVariable("ActualCapTorque", _machine.ActualCapTorque);
                    UpdateVariable("ActualCycleTime", _machine.ActualCycleTime);
                    UpdateVariable("FillAccuracyDeviation", _machine.FillAccuracyDeviation);

                    // System Status (dynamic)
                    UpdateVariable("MachineStatus", _machine.MachineStatus);
                    UpdateVariable("CurrentStation", _machine.CurrentStation);
                    UpdateVariable("ProductLevelTank", _machine.ProductLevelTank);
                    UpdateVariable("CleaningCycleStatus", _machine.CleaningCycleStatus);
                    UpdateVariable("QualityCheckWeight", _machine.QualityCheckWeight);
                    UpdateVariable("QualityCheckLevel", _machine.QualityCheckLevel);

                    // Counters - Global (dynamic)
                    UpdateVariable("GoodBottles", _machine.GoodBottles);
                    UpdateVariable("BadBottlesVolume", _machine.BadBottlesVolume);
                    UpdateVariable("BadBottlesWeight", _machine.BadBottlesWeight);
                    UpdateVariable("BadBottlesCap", _machine.BadBottlesCap);
                    UpdateVariable("BadBottlesOther", _machine.BadBottlesOther);
                    UpdateVariable("TotalBadBottles", _machine.TotalBadBottles);
                    UpdateVariable("TotalBottles", _machine.TotalBottles);

                    // Counters - Current Order (dynamic)
                    UpdateVariable("GoodBottlesOrder", _machine.GoodBottlesOrder);
                    UpdateVariable("BadBottlesOrder", _machine.BadBottlesOrder);
                    UpdateVariable("TotalBottlesOrder", _machine.TotalBottlesOrder);
                    UpdateVariable("ProductionOrderProgress", _machine.ProductionOrderProgress);

                    // Alarms (dynamic)
                    UpdateVariable("ActiveAlarms", _machine.ActiveAlarms.ToArray());
                    UpdateVariable("AlarmCount", (uint)_machine.ActiveAlarms.Count);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OPC variable update error: {ex.Message}");
            }
        }

        private void UpdateVariable(string name, object value)
        {
            if (_variables.ContainsKey(name))
            {
                _variables[name].Value = value;
                _variables[name].Timestamp = DateTime.UtcNow;
                _variables[name].ClearChangeMasks(SystemContext, false);
            }
        }

        private ServiceResult OnCallMethod(ISystemContext context, MethodState method, IList<object> inputArguments, IList<object> outputArguments)
        {
            try
            {
                string methodName = method.BrowseName.Name;

                switch (methodName)
                {
                    case "StartMachine":
                        _machine.StartMachine();
                        break;

                    case "StopMachine":
                        _machine.StopMachine();
                        break;

                    case "EnterMaintenanceMode":
                        _machine.EnterMaintenanceMode();
                        break;

                    case "StartCIPCycle":
                        _machine.StartCIPCycle();
                        break;

                    case "StartSIPCycle":
                        _machine.StartSIPCycle();
                        break;

                    case "ResetCounters":
                        _machine.ResetCounters();
                        break;

                    case "EmergencyStop":
                        _machine.EmergencyStop();
                        break;

                    case "GenerateLotNumber":
                        _machine.GenerateLotNumber();
                        break;

                    case "AdjustFillVolume":
                        if (inputArguments != null && inputArguments.Count > 0)
                        {
                            double newFillVolume = Convert.ToDouble(inputArguments[0]);
                            _machine.AdjustFillVolume(newFillVolume);
                        }
                        else
                        {
                            return StatusCodes.BadArgumentsMissing;
                        }
                        break;

                    case "LoadProductionOrder":
                        if (inputArguments != null && inputArguments.Count >= 9)
                        {
                            string orderNumber = Convert.ToString(inputArguments[0]);
                            string article = Convert.ToString(inputArguments[1]);
                            uint quantity = Convert.ToUInt32(inputArguments[2]);
                            double targetFillVolume = Convert.ToDouble(inputArguments[3]);
                            double targetLineSpeed = Convert.ToDouble(inputArguments[4]);
                            double targetProductTemp = Convert.ToDouble(inputArguments[5]);
                            double targetCO2Pressure = Convert.ToDouble(inputArguments[6]);
                            double targetCapTorque = Convert.ToDouble(inputArguments[7]);
                            double targetCycleTime = Convert.ToDouble(inputArguments[8]);

                            _machine.LoadProductionOrder(orderNumber, article, quantity, targetFillVolume,
                                targetLineSpeed, targetProductTemp, targetCO2Pressure, targetCapTorque, targetCycleTime);
                        }
                        else
                        {
                            return StatusCodes.BadArgumentsMissing;
                        }
                        break;

                    case "ChangeProduct":
                        if (inputArguments != null && inputArguments.Count >= 4)
                        {
                            string newArticle = Convert.ToString(inputArguments[0]);
                            double newTargetFillVolume = Convert.ToDouble(inputArguments[1]);
                            double newTargetProductTemp = Convert.ToDouble(inputArguments[2]);
                            double newTargetCO2Pressure = Convert.ToDouble(inputArguments[3]);

                            _machine.ChangeProduct(newArticle, newTargetFillVolume, newTargetProductTemp, newTargetCO2Pressure);
                        }
                        else
                        {
                            return StatusCodes.BadArgumentsMissing;
                        }
                        break;

                    default:
                        return StatusCodes.BadMethodInvalid;
                }

                return ServiceResult.Good;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Method execution error: {ex.Message}");
                return StatusCodes.BadInternalError;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _updateTimer?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}