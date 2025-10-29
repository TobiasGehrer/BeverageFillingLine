namespace OpcServer
{
    public class BeverageFillingLineMachine
    {
        private Random _random = new Random();

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
        private List<AlarmHistory> _alarmHistory = new List<AlarmHistory>();
        private int _cycleCount = 0;

        private class AlarmHistory
        {
            public string Parameter { get; set; }
            public double Value { get; set; }
            public DateTime Timestamp { get; set; }
            public double Target { get; set; }
            public double Deviation { get; set; }
        }

        public void UpdateSimulation()
        {
            if (MachineStatus != "Running")
            {
                return;
            }

            _cycleCount++;

            // Simulate realistic variations in actual values
            ActualFillVolume = TargetFillVolume + (_random.NextDouble() - 0.5) * 4.0;
            ActualLineSpeed = TargetLineSpeed + (_random.NextDouble() - 0.5) * 10.0;
            ActualProductTemperature = TargetProductTemperature + (_random.NextDouble() - 0.5) * 1.0;
            ActualCO2Pressure = TargetCO2Pressure + (_random.NextDouble() - 0.5) * 0.1;
            ActualCapTorque = TargetCapTorque + (_random.NextDouble() - 0.5) * 2.0;
            ActualCycleTime = TargetCycleTime + (_random.NextDouble() - 0.5) * 0.2;

            // Slowly decrease tank level
            ProductLevelTank = Math.Max(10.0, ProductLevelTank - 0.01);

            // Update current station (16 stations total)
            var stationNumber = (Environment.TickCount / 3000) % 16 + 1;
            CurrentStation = $"Station {stationNumber}";

            // Check for alarms
            CheckAlarms();
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

            // Special beverage industry alarms (immediate)
            CheckSpecialAlarms();

            // General parameter deviation alarms
            CheckParameterDeviations();

            // If any alarms are active, set machine status to Error
            // Removed: Because the whole simulation stopped updating values when in Error state
            //if (ActiveAlarms.Count > 0)
            //{
            //    MachineStatus = "Error";
            //}
        }

        private void RecordCycleData(string parameter, double actual, double target)
        {
            var history = new AlarmHistory
            {
                Parameter = parameter,
                Value = actual,
                Target = target,
                Deviation = Math.Abs(actual - target) / target * 100.0,
                Timestamp = DateTime.Now
            };

            _alarmHistory.Add(history);

            // Keep only last 10 cycles for each parameter
            _alarmHistory.RemoveAll(h => h.Parameter == parameter &&
                _alarmHistory.Count(x => x.Parameter == parameter) > 10);
        }

        private void CheckSpecialAlarms()
        {
            // Fill volume deviation alarm: If fill volume deviates more than ±1% from target
            if (Math.Abs(FillAccuracyDeviation / TargetFillVolume * 100) > 1.0)
            {
                ActiveAlarms.Add($"ALARM: Fill deviation {FillAccuracyDeviation:F2}ml exceeds ±1%");
            }

            // Product temperature alarm: If product temperature exceeds ±2°C from target
            if (Math.Abs(ActualProductTemperature - TargetProductTemperature) > 2.0)
            {
                ActiveAlarms.Add($"ALARM: Product temperature {ActualProductTemperature:F1}°C exceeds ±2°C");
            }

            // CO2 pressure alarm: If CO2 pressure deviates more than ±0.2 bar from target
            if (Math.Abs(ActualCO2Pressure - TargetCO2Pressure) > 0.2)
            {
                ActiveAlarms.Add($"ALARM: CO2 pressure {ActualCO2Pressure:F2} bar deviates more than ±0.2 bar");
            }

            // Product level low alarm: If tank level drops below 15%
            if (ProductLevelTank < 15.0)
            {
                ActiveAlarms.Add($"ALARM: Tank level {ProductLevelTank:F1}% too low (< 15%)");
            }

            // Cap torque alarm: If cap torque is outside ±10% of target range
            if (Math.Abs(ActualCapTorque - TargetCapTorque) / TargetCapTorque * 100 > 10.0)
            {
                ActiveAlarms.Add($"ALARM: Cap torque {ActualCapTorque:F1} Nm outside ±10% range");
            }
        }

        private void CheckParameterDeviations()
        {
            var parameters = new[] { "FillVolume", "LineSpeed", "ProductTemperature", "CO2Pressure", "CapTorque", "CycleTime" };

            foreach (var param in parameters)
            {
                var recentHistory = _alarmHistory.Where(h => h.Parameter == param).OrderByDescending(h => h.Timestamp).Take(3).ToList();

                if (recentHistory.Count >= 3)
                {
                    // Check for 3% deviation for 3 cycles in a row
                    if (recentHistory.All(h => h.Deviation > 3.0))
                    {
                        var values = string.Join(", ", recentHistory.Select(h => h.Value.ToString("F2")));
                        ActiveAlarms.Add($"ALARM: {param} deviation > 3% for 3 cycles. Values: {values}");
                    }
                }

                if (recentHistory.Count >= 1)
                {
                    // Check for 8% deviation for single cycle
                    if (recentHistory.First().Deviation > 8.0)
                    {
                        var recent = recentHistory.Take(3);
                        var values = string.Join(", ", recent.Select(h => h.Value.ToString("F2")));
                        ActiveAlarms.Add($"ALARM: {param} deviation > 8% in single cycle. Recent values: {values}");
                    }
                }
            }
        }

        // Machine control methods
        public void StartMachine()
        {
            if (MachineStatus == "Stopped")
            {
                MachineStatus = "Starting";
                Task.Delay(3000).ContinueWith(_ => MachineStatus = "Running");
            }
        }

        public void StopMachine()
        {
            if (MachineStatus == "Running" || MachineStatus == "Error" || MachineStatus == "Maintenance")
            {
                MachineStatus = "Stopping";
                Task.Delay(3000).ContinueWith(_ => MachineStatus = "Stopped");
            }
        }

        public void LoadProductionOrder(string orderNumber, string article, uint quantity,
            double targetFillVolume, double targetLineSpeed, double targetProductTemp,
            double targetCO2Pressure, double targetCapTorque, double targetCycleTime)
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
        }

        public void EnterMaintenanceMode()
        {
            MachineStatus = "Maintenance";
        }

        public void StartCIPCycle()
        {
            if (MachineStatus == "Stopped" || MachineStatus == "Maintenance")
            {
                CleaningCycleStatus = "CIP Active";

                // Simulate CIP cycle duration
                Task.Delay(60000).ContinueWith(_ => {
                    CleaningCycleStatus = "Normal Production";
                });
            }
        }

        public void StartSIPCycle()
        {
            if (MachineStatus == "Stopped" || MachineStatus == "Maintenance")
            {
                CleaningCycleStatus = "SIP Active";

                // Simulate SIP cycle duration
                Task.Delay(90000).ContinueWith(_ => {
                    CleaningCycleStatus = "Normal Production";
                });
            }
        }

        public void ResetCounters()
        {
            GoodBottles = 0;
            BadBottlesVolume = 0;
            BadBottlesWeight = 0;
            BadBottlesCap = 0;
            BadBottlesOther = 0;
        }

        public void ChangeProduct(string newArticle, double newTargetFillVolume,
            double newTargetProductTemp, double newTargetCO2Pressure)
        {
            // Simulate line purging and changeover
            MachineStatus = "Maintenance";
            CleaningCycleStatus = "Sanitizing";

            Task.Delay(30000).ContinueWith(_ => {
                Article = newArticle;
                TargetFillVolume = newTargetFillVolume;
                TargetProductTemperature = newTargetProductTemp;
                TargetCO2Pressure = newTargetCO2Pressure;
                CleaningCycleStatus = "Normal Production";
                MachineStatus = "Stopped";
            });
        }

        public void AdjustFillVolume(double newFillVolume)
        {
            if (Math.Abs(newFillVolume - TargetFillVolume) <= TargetFillVolume * 0.05) // Within 5%
            {
                TargetFillVolume = newFillVolume;
            }
        }

        public string GenerateLotNumber()
        {
            var now = DateTime.Now;
            var newLotNumber = $"LOT-{now:yyyy}-{Article?.Split('-')[1] ?? "UNK"}-{now:MMdd}{now.Hour:D2}";
            CurrentLotNumber = newLotNumber;
            return newLotNumber;
        }

        public void EmergencyStop()
        {
            MachineStatus = "Error";
            ActiveAlarms.Add("EMERGENCY STOP ACTIVATED - Manual intervention required");
        }
    }
}