namespace OpcServer
{
    public class BeverageFillingLineMachine
    {
        private Random _random = new Random();
        private int _cycleCount = 0;
        private DateTime _stateChangeTime = DateTime.MinValue;
        private int _errorRecoveryCycles = 0;

        // Simulation control
        private int _cyclesSinceLastIssue = 0;
        private string _simulationMode = "normal"; // "normal", "drift", "spike"
        private int _driftCycles = 0;
        private double _driftFactor = 1.0;

        // Machine Identification
        public string MachineName { get; set; } = "FluidFill Express #2";
        public string MachineSerialNumber { get; set; } = "FFE2000-2023-002";
        public string Plant { get; set; } = "Dortmund Beverage Center";
        public string ProductionSegment { get; set; } = "Non-Alcoholic Beverages";
        public string ProductionLine { get; set; } = "Juice Filling Line 3";

        // Production Order Information
        public string ProductionOrder { get; set; } = "PO-2024-JUICE-5567";
        public string Article { get; set; } = "ART-JUICE-APPLE-1L";
        public uint Quantity { get; set; } = 25000;

        // Target Values
        public double TargetFillVolume { get; set; } = 1000.0;
        public double TargetLineSpeed { get; set; } = 450.0;
        public double TargetProductTemperature { get; set; } = 6.5;
        public double TargetCO2Pressure { get; set; } = 3.8;
        public double TargetCapTorque { get; set; } = 22.0;
        public double TargetCycleTime { get; set; } = 2.67;

        // Actual Values
        public double ActualFillVolume { get; set; } = 999.2;
        public double ActualLineSpeed { get; set; } = 448.0;
        public double ActualProductTemperature { get; set; } = 6.3;
        public double ActualCO2Pressure { get; set; } = 3.75;
        public double ActualCapTorque { get; set; } = 21.8;
        public double ActualCycleTime { get; set; } = 2.68;

        // Calculated Values
        public double FillAccuracyDeviation => ActualFillVolume - TargetFillVolume;

        // System Status
        public double ProductLevelTank { get; set; } = 67.3;
        public string CleaningCycleStatus { get; set; } = "Normal Production";
        public string QualityCheckWeight { get; set; } = "Pass";
        public string QualityCheckLevel { get; set; } = "Pass";
        public string MachineStatus { get; set; } = "Running";
        public string CurrentStation { get; set; } = "Station 12";

        // Counters - Global (since startup)
        public uint GoodBottles { get; set; } = 1247589;
        public uint BadBottlesVolume { get; set; } = 284;
        public uint BadBottlesWeight { get; set; } = 192;
        public uint BadBottlesCap { get; set; } = 115;
        public uint BadBottlesOther { get; set; } = 73;
        public uint TotalBadBottles => BadBottlesVolume + BadBottlesWeight + BadBottlesCap + BadBottlesOther;
        public uint TotalBottles => GoodBottles + TotalBadBottles;

        // Counters - Current Order
        public uint GoodBottlesOrder { get; set; } = 12847;
        public uint BadBottlesOrder { get; set; } = 15;
        public uint TotalBottlesOrder => GoodBottlesOrder + BadBottlesOrder;
        public double ProductionOrderProgress => Quantity > 0 ? (double)TotalBottlesOrder / Quantity * 100.0 : 0.0;

        // Lot Information
        public string CurrentLotNumber { get; set; } = "LOT-2024-APPLE-0456";
        public DateTime ExpirationDate { get; set; } = new DateTime(2026, 9, 23);

        // Alarm system
        public List<string> ActiveAlarms { get; set; } = new List<string>();
        public uint AlarmCount => (uint)ActiveAlarms.Count;

        // SIMPLIFIED SIMULATION
        public void UpdateSimulation()
        {
            HandleStateTransitions();

            if (MachineStatus == "Error")
            {
                HandleErrorRecovery();
                return;
            }

            if (MachineStatus != "Running")
            {
                return;
            }

            _cycleCount++;
            _cyclesSinceLastIssue++;

            // Decide if we should trigger an issue
            DetermineSimulationMode();

            // Update all process values
            UpdateProcessValues();

            // Slowly decrease tank level
            ProductLevelTank = Math.Max(10.0, ProductLevelTank - 0.01);

            // Update current station (16 stations total)
            var stationNumber = (Environment.TickCount / 3000) % 16 + 1;
            CurrentStation = $"Station {stationNumber}";

            // Update production counters
            UpdateBottleProduction();

            // Check for alarms
            CheckAlarms();
        }

        private void DetermineSimulationMode()
        {
            // Normal operation most of the time
            if (_simulationMode == "normal")
            {
                // Every 10-15 minutes (600-900 seconds), start a drift
                if (_cyclesSinceLastIssue > 600 && _random.NextDouble() < 0.002)
                {
                    _simulationMode = "drift";
                    _driftCycles = 0;
                    _driftFactor = 1.0;
                    _cyclesSinceLastIssue = 0;
                    Console.WriteLine($"[MACHINE] Starting gradual drift at cycle {_cycleCount}");
                }
            }
            // During drift, gradually increase deviation
            else if (_simulationMode == "drift")
            {
                _driftCycles++;
                _driftFactor = 1.0 + (_driftCycles * 0.015); // Gradually increase to ~4% over 3 cycles

                // After drift causes alarm, return to normal
                if (_driftCycles > 5)
                {
                    _simulationMode = "normal";
                    _driftFactor = 1.0;
                    Console.WriteLine($"[MACHINE] Drift ended at cycle {_cycleCount}");
                }
            }
        }

        private void UpdateProcessValues()
        {
            // Base variation: very small (±0.5% to ±1%)
            double baseVariation = 0.01;

            // Apply drift factor if in drift mode
            double variation = baseVariation * _driftFactor;

            // Update each parameter with small random variation
            ActualFillVolume = TargetFillVolume * (1.0 + (_random.NextDouble() - 0.5) * variation);
            ActualLineSpeed = TargetLineSpeed * (1.0 + (_random.NextDouble() - 0.5) * variation);
            ActualProductTemperature = TargetProductTemperature * (1.0 + (_random.NextDouble() - 0.5) * variation);
            ActualCO2Pressure = TargetCO2Pressure * (1.0 + (_random.NextDouble() - 0.5) * variation);
            ActualCapTorque = TargetCapTorque * (1.0 + (_random.NextDouble() - 0.5) * variation);
            ActualCycleTime = TargetCycleTime * (1.0 + (_random.NextDouble() - 0.5) * variation);
        }

        private void HandleStateTransitions()
        {
            if (_stateChangeTime != DateTime.MinValue && DateTime.Now >= _stateChangeTime)
            {
                switch (MachineStatus)
                {
                    case "Starting":
                        MachineStatus = "Running";
                        Console.WriteLine("[MACHINE] Started successfully");
                        break;
                    case "Stopping":
                        MachineStatus = "Stopped";
                        Console.WriteLine("[MACHINE] Stopped successfully");
                        break;
                }

                _stateChangeTime = DateTime.MinValue;
            }
        }

        private void HandleErrorRecovery()
        {
            _errorRecoveryCycles++;

            // Gradually correct values back to targets
            ActualFillVolume = ActualFillVolume * 0.6 + TargetFillVolume * 0.4;
            ActualLineSpeed = ActualLineSpeed * 0.6 + TargetLineSpeed * 0.4;
            ActualProductTemperature = ActualProductTemperature * 0.6 + TargetProductTemperature * 0.4;
            ActualCO2Pressure = ActualCO2Pressure * 0.6 + TargetCO2Pressure * 0.4;
            ActualCapTorque = ActualCapTorque * 0.6 + TargetCapTorque * 0.4;

            // Refill tank if low
            if (ProductLevelTank < 50.0)
            {
                ProductLevelTank = Math.Min(100.0, ProductLevelTank + 2.0);
            }

            // After 5 cycles, clear alarms and return to running
            if (_errorRecoveryCycles >= 5)
            {
                Console.WriteLine($"[MACHINE] Error recovered after {_errorRecoveryCycles} cycles");
                ActiveAlarms.Clear();

                // Reset to clean state
                ActualFillVolume = TargetFillVolume;
                ActualLineSpeed = TargetLineSpeed;
                ActualProductTemperature = TargetProductTemperature;
                ActualCO2Pressure = TargetCO2Pressure;
                ActualCapTorque = TargetCapTorque;

                MachineStatus = "Running";
                _errorRecoveryCycles = 0;
                _simulationMode = "normal";
                _driftFactor = 1.0;
            }
        }

        private void UpdateBottleProduction()
        {
            // 99.8% pass rate = 0.2% failure rate (below 0.5% alarm threshold)
            bool passed = _random.NextDouble() < 0.998;

            if (passed)
            {
                GoodBottles++;
                GoodBottlesOrder++;
                QualityCheckWeight = "Pass";
                QualityCheckLevel = "Pass";
            }
            else
            {
                // Random failure type
                int failureType = _random.Next(4);

                switch (failureType)
                {
                    case 0: BadBottlesVolume++; break;
                    case 1: BadBottlesWeight++; break;
                    case 2: BadBottlesCap++; break;
                    case 3: BadBottlesOther++; break;
                }

                BadBottlesOrder++;
                QualityCheckWeight = _random.NextDouble() < 0.5 ? "Fail" : "Pass";
                QualityCheckLevel = _random.NextDouble() < 0.5 ? "Fail" : "Pass";
            }
        }

        private void CheckAlarms()
        {
            ActiveAlarms.Clear();

            // Fill volume deviation alarm: ±1%
            if (Math.Abs(FillAccuracyDeviation / TargetFillVolume * 100) > 1.0)
            {
                ActiveAlarms.Add($"ALARM: Fill deviation {FillAccuracyDeviation:F2}ml exceeds ±1%");
            }

            // Product temperature alarm: ±2°C
            if (Math.Abs(ActualProductTemperature - TargetProductTemperature) > 2.0)
            {
                ActiveAlarms.Add($"ALARM: Product temperature {ActualProductTemperature:F1}°C exceeds ±2°C");
            }

            // CO2 pressure alarm: ±0.2 bar
            if (Math.Abs(ActualCO2Pressure - TargetCO2Pressure) > 0.2)
            {
                ActiveAlarms.Add($"ALARM: CO2 pressure {ActualCO2Pressure:F2} bar deviates more than ±0.2 bar");
            }

            // Product level low alarm: < 15%
            if (ProductLevelTank < 15.0)
            {
                ActiveAlarms.Add($"ALARM: Tank level {ProductLevelTank:F1}% too low (< 15%)");
            }

            // Cap torque alarm: ±10%
            if (Math.Abs(ActualCapTorque - TargetCapTorque) / TargetCapTorque * 100 > 10.0)
            {
                ActiveAlarms.Add($"ALARM: Cap torque {ActualCapTorque:F1} Nm outside ±10% range");
            }

            // Quality control alarm: > 0.5% failure rate
            if (TotalBottles > 1000)
            {
                double failureRate = (double)TotalBadBottles / TotalBottles * 100.0;
                if (failureRate > 0.5)
                {
                    ActiveAlarms.Add($"ALARM: Quality failure rate {failureRate:F2}% exceeds 0.5%");
                }
            }

            // Set machine to Error if alarms detected
            if (ActiveAlarms.Count > 0 && MachineStatus == "Running")
            {
                MachineStatus = "Error";
            }
        }

        // Machine control methods
        public void StartMachine()
        {
            if (MachineStatus == "Stopped")
            {
                MachineStatus = "Starting";
                _stateChangeTime = DateTime.Now.AddSeconds(3);
                Console.WriteLine("[MACHINE] Starting...");
            }
        }

        public void StopMachine()
        {
            if (MachineStatus == "Running" || MachineStatus == "Error" || MachineStatus == "Maintenance")
            {
                MachineStatus = "Stopping";
                _stateChangeTime = DateTime.Now.AddSeconds(3);
                Console.WriteLine("[MACHINE] Stopping...");
            }
        }

        public void LoadProductionOrder(string orderNumber, string article, uint quantity, double targetFillVolume, double targetLineSpeed, double targetProductTemp, double targetCO2Pressure, double targetCapTorque, double targetCycleTime)
        {
            ProductionOrder = orderNumber;
            Article = article;
            Quantity = quantity;
            TargetFillVolume = targetFillVolume;
            TargetLineSpeed = targetLineSpeed;
            TargetProductTemperature = targetProductTemp;
            TargetCO2Pressure = targetCO2Pressure;
            TargetCapTorque = targetCapTorque;
            TargetCycleTime = targetCycleTime;

            // Reset order counters
            GoodBottlesOrder = 0;
            BadBottlesOrder = 0;

            Console.WriteLine($"[MACHINE] Loaded production order: {orderNumber}");
        }

        public void EnterMaintenanceMode()
        {
            MachineStatus = "Maintenance";
            Console.WriteLine("[MACHINE] Entered maintenance mode");
        }

        public void StartCleaningCycle(string cycleType)
        {
            CleaningCycleStatus = cycleType;
            Console.WriteLine($"[MACHINE] Started {cycleType}");
        }

        public void StartCIPCycle()
        {
            StartCleaningCycle("CIP Active");
        }

        public void StartSIPCycle()
        {
            StartCleaningCycle("SIP Active");
        }

        public void ResetCounters()
        {
            GoodBottles = 0;
            BadBottlesVolume = 0;
            BadBottlesWeight = 0;
            BadBottlesCap = 0;
            BadBottlesOther = 0;
            Console.WriteLine("[MACHINE] Counters reset");
        }

        public void ChangeProduct(string newArticle, double newTargetFillVolume, double newTargetProductTemp, double newTargetCO2Pressure)
        {
            Article = newArticle;
            TargetFillVolume = newTargetFillVolume;
            TargetProductTemperature = newTargetProductTemp;
            TargetCO2Pressure = newTargetCO2Pressure;
            Console.WriteLine($"[MACHINE] Changed product to: {newArticle}");
        }

        public void AdjustFillVolume(double newFillVolume)
        {
            if (Math.Abs(newFillVolume - TargetFillVolume) <= TargetFillVolume * 0.05)
            {
                TargetFillVolume = newFillVolume;
                Console.WriteLine($"[MACHINE] Fill volume adjusted to {newFillVolume}ml");
            }
        }

        public string GenerateLotNumber()
        {
            var now = DateTime.Now;
            var newLotNumber = $"LOT-{now:yyyy}-{Article?.Split('-')[1] ?? "UNK"}-{now:MMdd}{now.Hour:D2}";
            CurrentLotNumber = newLotNumber;
            Console.WriteLine($"[MACHINE] Generated lot number: {newLotNumber}");
            return newLotNumber;
        }

        public void EmergencyStop()
        {
            MachineStatus = "Error";
            ActiveAlarms.Add("EMERGENCY STOP ACTIVATED - Manual intervention required");
            Console.WriteLine("[MACHINE] EMERGENCY STOP!");
        }
    }
}