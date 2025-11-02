using Opc.Ua;
using Opc.Ua.Server;
using System.Reflection;

namespace OpcServer
{
    public class BeverageFillingLineNodeManager : CustomNodeManager2
    {
        private BeverageFillingLineMachine _machine;
        private Dictionary<string, BaseDataVariableState> _variables;
        private Timer _updateTimer;
        private HashSet<string> _staticProperties;

        public BeverageFillingLineNodeManager(IServerInternal server, ApplicationConfiguration configuration, BeverageFillingLineMachine machine) : base(server, configuration, "urn:BeverageServer:")
        {
            _machine = machine;
            _variables = new Dictionary<string, BaseDataVariableState>();
            _staticProperties = new HashSet<string> { "MachineName", "MachineSerialNumber", "Plant", "ProductionSegment", "ProductionLine" };
            SetNamespaces("urn:BeverageServer:");
        }

        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            lock (Lock)
            {
                LoadPredefinedNodes(SystemContext, externalReferences);
                _updateTimer = new Timer(UpdateOpcVariables, null, 0, 1000);
            }
        }

        protected override NodeStateCollection LoadPredefinedNodes(ISystemContext context)
        {
            NodeStateCollection predefinedNodes = new NodeStateCollection();

            try
            {                
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

                // Auto-create variables from machine properties
                CreateVariablesFromProperties(root, predefinedNodes);
                
                FolderState methodsFolder = new FolderState(root)
                {
                    NodeId = new NodeId("Methods", NamespaceIndex),
                    BrowseName = new QualifiedName("Methods", NamespaceIndex),
                    DisplayName = new LocalizedText("Methods"),
                    TypeDefinitionId = ObjectTypeIds.FolderType
                };

                root.AddChild(methodsFolder);
                AddPredefinedNode(context, methodsFolder);
                predefinedNodes.Add(methodsFolder);

                // Create methods
                CreateMethod(methodsFolder, "StartMachine", "Starts the machine", predefinedNodes);
                CreateMethod(methodsFolder, "StopMachine", "Stops the machine", predefinedNodes);
                CreateMethod(methodsFolder, "EnterMaintenanceMode", "Enters maintenance mode", predefinedNodes);
                CreateMethod(methodsFolder, "StartCIPCycle", "Starts Clean-in-Place cycle", predefinedNodes);
                CreateMethod(methodsFolder, "StartSIPCycle", "Starts Sterilize-in-Place cycle", predefinedNodes);
                CreateMethod(methodsFolder, "ResetCounters", "Resets production counters", predefinedNodes);
                CreateMethod(methodsFolder, "EmergencyStop", "Emergency stop", predefinedNodes);
                CreateMethod(methodsFolder, "GenerateLotNumber", "Generates new lot number", predefinedNodes);

                CreateMethodWithParameters(methodsFolder, "AdjustFillVolume", "Adjusts fill volume", predefinedNodes, new List<Argument> {
                        new Argument("newFillVolume", DataTypeIds.Double, ValueRanks.Scalar, "New fill volume in ml")
                    });

                CreateMethodWithParameters(methodsFolder, "LoadProductionOrder", "Loads a new production order", predefinedNodes, new List<Argument> {
                        new Argument("orderNumber", DataTypeIds.String, ValueRanks.Scalar, "Production order number"),
                        new Argument("article", DataTypeIds.String, ValueRanks.Scalar, "Article code"),
                        new Argument("quantity", DataTypeIds.UInt32, ValueRanks.Scalar, "Target quantity"),
                        new Argument("targetFillVolume", DataTypeIds.Double, ValueRanks.Scalar, "Target fill volume"),
                        new Argument("targetLineSpeed", DataTypeIds.Double, ValueRanks.Scalar, "Target line speed"),
                        new Argument("targetProductTemp", DataTypeIds.Double, ValueRanks.Scalar, "Target product temperature"),
                        new Argument("targetCO2Pressure", DataTypeIds.Double, ValueRanks.Scalar, "Target CO2 pressure"),
                        new Argument("targetCapTorque", DataTypeIds.Double, ValueRanks.Scalar, "Target cap torque"),
                        new Argument("targetCycleTime", DataTypeIds.Double, ValueRanks.Scalar, "Target cycle time")
                    });

                CreateMethodWithParameters(methodsFolder, "ChangeProduct", "Changes product specifications", predefinedNodes, new List<Argument> {
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

        private void CreateVariablesFromProperties(FolderState parent, NodeStateCollection predefinedNodes)
        {
            var properties = typeof(BeverageFillingLineMachine).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var prop in properties)
            {
                var value = prop.GetValue(_machine);
                if (value == null)
                {
                    continue;
                }

                NodeId dataType = GetOpcDataType(prop.PropertyType);

                if (dataType == null)
                {
                    continue;
                }

                bool isArray = prop.PropertyType.IsArray || (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(List<>));

                if (isArray)
                {
                    CreateArrayVariable(parent, prop.Name, dataType, value, predefinedNodes);
                }
                else
                {
                    CreateVariable(parent, prop.Name, dataType, value, predefinedNodes);
                }
            }
        }

        private NodeId GetOpcDataType(Type type)
        {
            if (type == typeof(string))
            {
                return DataTypeIds.String;
            }

            if (type == typeof(double))
            {
                return DataTypeIds.Double;
            }

            if (type == typeof(uint))
            {
                return DataTypeIds.UInt32;
            }

            if (type == typeof(DateTime))
            {
                return DataTypeIds.DateTime;
            }

            if (type == typeof(string[]))
            {
                return DataTypeIds.String;
            }

            if (type == typeof(List<string>))
            {
                return DataTypeIds.String;
            }

            return null;
        }

        private void CreateMethod(FolderState parent, string name, string description, NodeStateCollection predefinedNodes)
        {
            var method = new MethodState(parent)
            {
                NodeId = new NodeId(name, NamespaceIndex),
                BrowseName = new QualifiedName(name, NamespaceIndex),
                DisplayName = new LocalizedText(name),
                Description = new LocalizedText(description),
                Executable = true,
                UserExecutable = true,
                OnCallMethod = new GenericMethodCalledEventHandler(OnCallMethod)
            };

            parent.AddChild(method);
            AddPredefinedNode(SystemContext, method);
            predefinedNodes.Add(method);
            method.AddReference(ReferenceTypes.HasComponent, true, parent.NodeId);
        }

        private void CreateMethodWithParameters(FolderState parent, string name, string description,
            NodeStateCollection predefinedNodes, List<Argument> inputArguments)
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

            parent.AddChild(method);
            AddPredefinedNode(SystemContext, method);
            predefinedNodes.Add(method);
            method.AddReference(ReferenceTypes.HasComponent, true, parent.NodeId);
        }

        private void CreateArrayVariable(FolderState parent, string name, NodeId dataType, object initialValue, NodeStateCollection predefinedNodes)
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

        private void CreateVariable(FolderState parent, string name, NodeId dataType, object initialValue, NodeStateCollection predefinedNodes)
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

        private void UpdateOpcVariables(object state)
        {
            try
            {
                lock (Lock)
                {
                    var properties = typeof(BeverageFillingLineMachine).GetProperties(BindingFlags.Public | BindingFlags.Instance);

                    foreach (var prop in properties)
                    {                        
                        if (_staticProperties.Contains(prop.Name))
                        {
                            continue;
                        }

                        var value = prop.GetValue(_machine);

                        if (value != null && _variables.ContainsKey(prop.Name))
                        {
                            _variables[prop.Name].Value = value;
                            _variables[prop.Name].Timestamp = DateTime.UtcNow;
                            _variables[prop.Name].ClearChangeMasks(SystemContext, false);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OPC variable update error: {ex.Message}");
            }
        }

        private ServiceResult OnCallMethod(ISystemContext context, MethodState method, IList<object> inputArguments, IList<object> outputArguments)
        {
            try
            {
                switch (method.BrowseName.Name)
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
                        if (inputArguments?.Count > 0)
                        {
                            _machine.AdjustFillVolume(Convert.ToDouble(inputArguments[0]));
                        }
                        else
                        {
                            return StatusCodes.BadArgumentsMissing;
                        }
                        break;

                    case "LoadProductionOrder":
                        if (inputArguments?.Count >= 9)
                        {
                            _machine.LoadProductionOrder(
                                Convert.ToString(inputArguments[0]),
                                Convert.ToString(inputArguments[1]),
                                Convert.ToUInt32(inputArguments[2]),
                                Convert.ToDouble(inputArguments[3]),
                                Convert.ToDouble(inputArguments[4]),
                                Convert.ToDouble(inputArguments[5]),
                                Convert.ToDouble(inputArguments[6]),
                                Convert.ToDouble(inputArguments[7]),
                                Convert.ToDouble(inputArguments[8]));
                        }
                        else
                        {
                            return StatusCodes.BadArgumentsMissing;
                        }
                        break;

                    case "ChangeProduct":
                        if (inputArguments?.Count >= 4)
                        {
                            _machine.ChangeProduct(
                                Convert.ToString(inputArguments[0]),
                                Convert.ToDouble(inputArguments[1]),
                                Convert.ToDouble(inputArguments[2]),
                                Convert.ToDouble(inputArguments[3]));
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