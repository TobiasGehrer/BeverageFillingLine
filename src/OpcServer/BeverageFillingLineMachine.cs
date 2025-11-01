namespace OpcServer
{
    public class BeverageFillingLineMachine
    {
        private Random _random = new Random();
        private Dictionary<string, Queue<double>> _deviationHistory = new Dictionary<string, Queue<double>>();
        private int _cycleCount = 0;      
        private int _cyclesSinceLastError = 0;
        private int _errorRecoveryCycles = 0;      
        private DateTime _stateChangeTime = DateTime.MinValue;

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
        public uint BadBottlesVolume { get; set; } = 2847;
        public uint BadBottlesWeight { get; set; } = 1923;
        public uint BadBottlesCap { get; set; } = 1156;
        public uint BadBottlesOther { get; set; } = 734;
        public uint TotalBadBottles => BadBottlesVolume + BadBottlesWeight + BadBottlesCap + BadBottlesOther;
        public uint TotalBottles => GoodBottles + TotalBadBottles;

        // Counters - Current Order
        public uint GoodBottlesOrder { get; set; } = 12847;
        public uint BadBottlesOrder { get; set; } = 153;
        public uint TotalBottlesOrder => GoodBottlesOrder + BadBottlesOrder;
        public double ProductionOrderProgress => Quantity > 0 ? (double)TotalBottlesOrder / Quantity * 100.0 : 0.0;

        // Lot Information
        public string CurrentLotNumber { get; set; } = "LOT-2024-APPLE-0456";
        public DateTime ExpirationDate { get; set; } = new DateTime(2026, 9, 23);

        // Alarm system
        public List<string> ActiveAlarms { get; set; } = new List<string>();
        public uint AlarmCount => (uint)ActiveAlarms.Count;


        // SIMULATION
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
            _cyclesSinceLastError++;

            // Randomly cause bigger deviations every 200-400 cycles
            double variation = 1.0;

            if (_cyclesSinceLastError > 200 && _random.NextDouble() < 0.005)
            {
                variation = _random.Next(5, 10);
                _cyclesSinceLastError = 0;
                Console.WriteLine($"[MACHINE] Process upset at cycle {_cycleCount}!");
            }
            
            // Simulate realistic variations
            ActualFillVolume = TargetFillVolume + (_random.NextDouble() - 0.5) * 4.0 * variation;
            ActualLineSpeed = TargetLineSpeed + (_random.NextDouble() - 0.5) * 10.0 * variation;
            ActualProductTemperature = TargetProductTemperature + (_random.NextDouble() - 0.5) * 1.0 * variation;
            ActualCO2Pressure = TargetCO2Pressure + (_random.NextDouble() - 0.5) * 0.1 * variation;
            ActualCapTorque = TargetCapTorque + (_random.NextDouble() - 0.5) * 2.0 * variation;
            ActualCycleTime = TargetCycleTime + (_random.NextDouble() - 0.5) * 0.2 * variation;

            // Slowly decrease tank level
            ProductLevelTank = Math.Max(10.0, ProductLevelTank - 0.01);

            // Update current station (16 stations total)
            var stationNumber = (Environment.TickCount / 3000) % 16 + 1;
            CurrentStation = $"Station {stationNumber}";

            UpdateBottleProduction();
            CheckAlarms();
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

            // Correct values back to targets over 5 cycles           
            ActualFillVolume = ActualFillVolume * 0.7 + TargetFillVolume * 0.3;
            ActualLineSpeed = ActualLineSpeed * 0.7 + TargetLineSpeed * 0.3;
            ActualProductTemperature = ActualProductTemperature * 0.7 + TargetProductTemperature * 0.3;
            ActualCO2Pressure = ActualCO2Pressure * 0.7 + TargetCO2Pressure * 0.3;
            ActualCapTorque = ActualCapTorque * 0.7 + TargetCapTorque * 0.3;

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
                MachineStatus = "Running";
                _errorRecoveryCycles = 0;
            }
        }

        private void UpdateBottleProduction()
        {
            // Simulate bottle production every cycle - 98% pass rate
            bool passed = _random.NextDouble() < 0.98;

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

            // Record current cycle data
            RecordCycleData("FillVolume", ActualFillVolume, TargetFillVolume);
            RecordCycleData("LineSpeed", ActualLineSpeed, TargetLineSpeed);
            RecordCycleData("ProductTemperature", ActualProductTemperature, TargetProductTemperature);
            RecordCycleData("CO2Pressure", ActualCO2Pressure, TargetCO2Pressure);
            RecordCycleData("CapTorque", ActualCapTorque, TargetCapTorque);
            RecordCycleData("CycleTime", ActualCycleTime, TargetCycleTime);

            // Special beverage industry alarms
            CheckSpecialAlarms();

            // General parameter deviation alarms
            CheckParameterDeviations();

            // Quality control alarm
            if (TotalBottles > 0)
            {
                double failureRate = (double)TotalBadBottles / TotalBottles * 100.0;
                if (failureRate > 0.5)
                {
                    ActiveAlarms.Add($"ALARM: Quality failure rate {failureRate:F2}% exceeds 0.5%");
                }
            }

            // Set machine to Error if critical alarms detected
            if (ActiveAlarms.Count > 0 && MachineStatus == "Running")
                MachineStatus = "Error";
        }

        private void RecordCycleData(string parameter, double actual, double target)
        {
            if (!_deviationHistory.ContainsKey(parameter))
            {
                _deviationHistory[parameter] = new Queue<double>();
            }

            double deviation = Math.Abs(actual - target) / target * 100.0;
            var queue = _deviationHistory[parameter];

            queue.Enqueue(deviation);
            while (queue.Count > 3)
            {
                queue.Dequeue();
            }
        }

        private void CheckSpecialAlarms()
        {
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
        }

        private void CheckParameterDeviations()
        {
            foreach (var param in _deviationHistory.Keys)
            {
                var deviations = _deviationHistory[param].ToList();

                // Check for 3% deviation for 3 cycles in a row
                if (deviations.Count >= 3 && deviations.All(d => d > 3.0))
                {
                    ActiveAlarms.Add($"ALARM: {param} deviation > 3% for 3 cycles");
                }

                // Check for 8% deviation in single cycle
                if (deviations.Count >= 1 && deviations.Last() > 8.0)
                {
                    ActiveAlarms.Add($"ALARM: {param} deviation > 8% in single cycle");
                }
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