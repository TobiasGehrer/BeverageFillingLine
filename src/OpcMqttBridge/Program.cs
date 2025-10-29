using System.Text.Json;

namespace OpcMqttBridge
{
    class Program
    {
        private static OpcUaClient? _opcUaClient;
        private static MqttPublisher? _mqttPublisher;
        private static Timer? _publishTimer;

        // UNS Configuration - adjust these to match your organization
        private const string Version = "v1";
        private const string Enterprise = "best-beverage";
        private const string Site = "dornbirn";
        private const string Area = "production";
        private const string Line = "filling-line-1";

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== OPC UA to MQTT Bridge Agent ===");
            Console.WriteLine("Connecting beverage filling line to MQTT broker...\n");

            try
            {
                // Get configuration from environment variables
                string opcServerUrl = Environment.GetEnvironmentVariable("OPC_SERVER_URL") ?? "opc.tcp://localhost:4840";
                string mqttBroker = Environment.GetEnvironmentVariable("MQTT_BROKER") ?? "localhost";
                int mqttPort = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");

                Console.WriteLine($"Connecting to OPC Server: {opcServerUrl}");
                Console.WriteLine($"Connecting to MQTT Broker: {mqttBroker}:{mqttPort}\n");

                // Initialize OPC UA client
                _opcUaClient = new OpcUaClient(opcServerUrl, "OpcMqttBridge");
                await _opcUaClient.ConnectAsync();
                Console.WriteLine("Connected to OPC UA server");

                // Initialize MQTT publisher
                _mqttPublisher = new MqttPublisher(mqttBroker, mqttPort, "beverage-filling-line-bridge");
                await _mqttPublisher.ConnectAsync();
                Console.WriteLine("Connected to MQTT broker\n");

                // Start periodic publishing
                _publishTimer = new Timer(async _ => await PublishAllDataAsync(), null, TimeSpan.Zero, TimeSpan.FromSeconds(3));

                Console.WriteLine("Bridge active. Publishing every 3 seconds...");
                Console.WriteLine($"UNS Base Topic: {Version}/{Enterprise}/{Site}/{Area}/{Line}");
                Console.WriteLine("Press Ctrl+C to stop.\n");

                // Keep running
                await Task.Delay(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                _publishTimer?.Dispose();
                if (_mqttPublisher != null)
                {
                    await _mqttPublisher.DisconnectAsync();
                }
                if (_opcUaClient != null)
                {
                    await _opcUaClient.DisconnectAsync();
                }

                // Give time for cleanup
                await Task.Delay(500);
            }
        }

        private static async Task PublishAllDataAsync()
        {
            try
            {
                var timestamp = DateTime.UtcNow;

                // Read all OPC UA variables
                var machineData = await _opcUaClient!.ReadAllVariablesAsync();

                // Publish each variable individually following UNS structure
                int publishCount = 0;
                foreach (var mapping in GetTopicMappings())
                {
                    if (machineData.TryGetValue(mapping.OpcVariable, out var value))
                    {
                        await PublishMetric(mapping.Topic, value, timestamp);
                        publishCount++;
                    }
                }

                Console.WriteLine($"[{timestamp:HH:mm:ss}] Published {publishCount} metrics to MQTT");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Publish error: {ex.Message}");
            }
        }

        private static async Task PublishMetric(string topic, object value, DateTime timestamp)
        {
            var payload = new
            {
                timestamp,
                value
            };

            var fullTopic = $"{Version}/{Enterprise}/{Site}/{Area}/{Line}/{topic}";
            await _mqttPublisher!.PublishAsync(fullTopic, JsonSerializer.Serialize(payload));
        }

        private static List<TopicMapping> GetTopicMappings()
        {
            return new List<TopicMapping>
            {
                // Machine Information
                new("machine_name", "MachineName"),
                new("machine_serial_number", "MachineSerialNumber"),
                new("machine_plant", "Plant"),
                new("machine_status", "MachineStatus"),
                new("machine_current_station", "CurrentStation"),
                new("machine_cleaning_status", "CleaningCycleStatus"),

                // Production Order Information
                new("production_order", "ProductionOrder"),
                new("production_article", "Article"),
                new("production_quantity", "Quantity"),
                new("production_lot_number", "CurrentLotNumber"),
                new("production_expiration_date", "ExpirationDate"),
                new("production_progress_percent", "ProductionOrderProgress"),

                // Production Counters
                new("counters_good_bottles", "GoodBottles"),
                new("counters_bad_bottles_total", "TotalBadBottles"),
                new("counters_bad_bottles_volume", "BadBottlesVolume"),
                new("counters_bad_bottles_weight", "BadBottlesWeight"),
                new("counters_bad_bottles_cap", "BadBottlesCap"),
                new("counters_bad_bottles_other", "BadBottlesOther"),
                new("counters_total_bottles", "TotalBottles"),
                new("counters_good_bottles_order", "GoodBottlesOrder"),
                new("counters_bad_bottles_order", "BadBottlesOrder"),

                // Process Values - Fill Volume
                new("process_fill_volume_target", "TargetFillVolume"),
                new("process_fill_volume_actual", "ActualFillVolume"),
                new("process_fill_volume_deviation", "FillAccuracyDeviation"),

                // Process Values - Line Speed
                new("process_line_speed_target", "TargetLineSpeed"),
                new("process_line_speed_actual", "ActualLineSpeed"),

                // Process Values - Temperature
                new("process_temperature_target", "TargetProductTemperature"),
                new("process_temperature_actual", "ActualProductTemperature"),

                // Process Values - CO2 Pressure
                new("process_co2_pressure_target", "TargetCO2Pressure"),
                new("process_co2_pressure_actual", "ActualCO2Pressure"),

                // Process Values - Cap Torque
                new("process_cap_torque_target", "TargetCapTorque"),
                new("process_cap_torque_actual", "ActualCapTorque"),

                // Process Values - Cycle Time
                new("process_cycle_time_target", "TargetCycleTime"),
                new("process_cycle_time_actual", "ActualCycleTime"),

                // Process Values - Tank Level
                new("process_tank_level_percent", "ProductLevelTank"),

                // Quality Checks
                new("quality_weight_check", "QualityCheckWeight"),
                new("quality_level_check", "QualityCheckLevel"),

                // Alarms
                new("alarms_count", "AlarmCount"),
                new("alarms_active", "ActiveAlarms")
            };
        }

        private record TopicMapping(string Topic, string OpcVariable);
    }
}