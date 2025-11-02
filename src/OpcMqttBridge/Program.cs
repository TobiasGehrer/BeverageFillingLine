using System.Text.Json;
using System.Collections.Concurrent;

namespace OpcMqttBridge
{
    class Program
    {
        private static OpcUaClient? _opcUaClient;
        private static MqttPublisher? _mqttPublisher;
        private static CancellationTokenSource? _cts;

        // Thread-safe queue for batching messages
        private static readonly ConcurrentQueue<PendingMessage> _messageQueue = new();
        private static Timer? _batchTimer;
        private static int _publishedCount = 0;
        private static DateTime _lastStatsTime = DateTime.UtcNow;

        // UNS Configuration - adjust these to match your organization
        private const string Version = "v1";
        private const string Enterprise = "best-beverage";
        private const string Site = "dornbirn";
        private const string Area = "production";
        private const string Line = "filling-line-1";

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== OPC UA to MQTT Bridge (Subscription-Based) ===");
            Console.WriteLine("Connecting beverage filling line to MQTT broker...\n");

            _cts = new CancellationTokenSource();

            // Handle graceful shutdown
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("\n\nShutdown requested...");
                _cts?.Cancel();
            };

            try
            {
                // Get configuration from environment variables
                string opcServerUrl = Environment.GetEnvironmentVariable("OPC_SERVER_URL") ?? "opc.tcp://localhost:4840";
                string mqttBroker = Environment.GetEnvironmentVariable("MQTT_BROKER") ?? "localhost";
                int mqttPort = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
                int publishingInterval = int.Parse(Environment.GetEnvironmentVariable("OPC_PUBLISHING_INTERVAL") ?? "1000");
                int batchIntervalMs = int.Parse(Environment.GetEnvironmentVariable("MQTT_BATCH_INTERVAL") ?? "100");

                Console.WriteLine($"OPC Server: {opcServerUrl}");
                Console.WriteLine($"MQTT Broker: {mqttBroker}:{mqttPort}");
                Console.WriteLine($"OPC Publishing Interval: {publishingInterval}ms");
                Console.WriteLine($"MQTT Batch Interval: {batchIntervalMs}ms\n");

                // Initialize OPC UA client
                _opcUaClient = new OpcUaClient(opcServerUrl, "OpcMqttBridge");
                await ConnectOpcWithRetry(_cts.Token);

                // Initialize MQTT publisher
                _mqttPublisher = new MqttPublisher(mqttBroker, mqttPort, "beverage-filling-line-bridge");
                await ConnectMqttWithRetry(_cts.Token);

                // Subscribe to value changes
                _opcUaClient.ValueChanged += OnOpcValueChanged;

                // Create OPC UA subscription
                await _opcUaClient.CreateSubscriptionAsync(publishingInterval);

                Console.WriteLine("✓ Subscription-based bridge active!");
                Console.WriteLine($"✓ UNS Base Topic: {Version}/{Enterprise}/{Site}/{Area}/{Line}");
                Console.WriteLine("✓ Publishing only when values change\n");
                Console.WriteLine("Press Ctrl+C to stop.\n");

                // Start batch publishing timer
                _batchTimer = new Timer(PublishBatchedMessages, null,
                    TimeSpan.FromMilliseconds(batchIntervalMs),
                    TimeSpan.FromMilliseconds(batchIntervalMs));

                // Keep running and monitor connection
                await MonitorConnectionAsync(_cts.Token);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                await CleanupAsync();
            }
        }

        private static async Task ConnectOpcWithRetry(CancellationToken cancellationToken)
        {
            int retryCount = 0;
            int maxRetries = 10;
            int delaySeconds = 5;

            while (!cancellationToken.IsCancellationRequested && retryCount < maxRetries)
            {
                try
                {
                    await _opcUaClient!.ConnectAsync();
                    Console.WriteLine("✓ Connected to OPC UA server\n");
                    return;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Console.WriteLine($"✗ Failed to connect to OPC UA server (attempt {retryCount}/{maxRetries}): {ex.Message}");

                    if (retryCount < maxRetries)
                    {
                        Console.WriteLine($"  Retrying in {delaySeconds} seconds...");
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
                        delaySeconds = Math.Min(delaySeconds * 2, 60); // Exponential backoff
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private static async Task ConnectMqttWithRetry(CancellationToken cancellationToken)
        {
            int retryCount = 0;
            int maxRetries = 10;
            int delaySeconds = 5;

            while (!cancellationToken.IsCancellationRequested && retryCount < maxRetries)
            {
                try
                {
                    await _mqttPublisher!.ConnectAsync();
                    Console.WriteLine("✓ Connected to MQTT broker\n");
                    return;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Console.WriteLine($"✗ Failed to connect to MQTT broker (attempt {retryCount}/{maxRetries}): {ex.Message}");

                    if (retryCount < maxRetries)
                    {
                        Console.WriteLine($"  Retrying in {delaySeconds} seconds...");
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
                        delaySeconds = Math.Min(delaySeconds * 2, 60); // Exponential backoff
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private static void OnOpcValueChanged(object? sender, ValueChangedEventArgs e)
        {
            try
            {
                // Map OPC variable name to MQTT topic
                var topicMapping = GetTopicMappings().FirstOrDefault(m => m.OpcVariable == e.NodeName);

                if (topicMapping != null)
                {
                    // Add to queue for batched publishing
                    _messageQueue.Enqueue(new PendingMessage
                    {
                        Topic = topicMapping.Topic,
                        Value = e.Value,
                        Timestamp = e.Timestamp == DateTime.MinValue ? DateTime.UtcNow : e.Timestamp
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling value change: {ex.Message}");
            }
        }

        private static async void PublishBatchedMessages(object? state)
        {
            if (_messageQueue.IsEmpty || _mqttPublisher == null)
                return;

            var batch = new List<PendingMessage>();

            // Dequeue all pending messages
            while (_messageQueue.TryDequeue(out var message))
            {
                batch.Add(message);
            }

            if (batch.Count == 0)
                return;

            // Publish all messages in batch
            var publishTasks = batch.Select(msg => PublishMetricAsync(msg.Topic, msg.Value, msg.Timestamp));

            try
            {
                await Task.WhenAll(publishTasks);

                _publishedCount += batch.Count;

                // Print stats every 10 seconds
                var elapsed = DateTime.UtcNow - _lastStatsTime;
                if (elapsed.TotalSeconds >= 10)
                {
                    var rate = _publishedCount / elapsed.TotalSeconds;
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Published {_publishedCount} metrics ({rate:F1} msg/s)");
                    _publishedCount = 0;
                    _lastStatsTime = DateTime.UtcNow;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Batch publish error: {ex.Message}");
            }
        }

        private static async Task PublishMetricAsync(string topic, object value, DateTime timestamp)
        {
            try
            {
                var payload = new
                {
                    timestamp,
                    value
                };

                var fullTopic = $"{Version}/{Enterprise}/{Site}/{Area}/{Line}/{topic}";
                await _mqttPublisher!.PublishAsync(fullTopic, JsonSerializer.Serialize(payload));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Publish error for {topic}: {ex.Message}");
            }
        }

        private static async Task MonitorConnectionAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(5000, cancellationToken);

                    // Check OPC connection
                    if (_opcUaClient != null && !_opcUaClient.IsConnected())
                    {
                        Console.WriteLine("OPC connection lost, attempting reconnect...");
                        await ConnectOpcWithRetry(cancellationToken);
                        if (_opcUaClient.IsConnected())
                        {
                            await _opcUaClient.CreateSubscriptionAsync();
                        }
                    }

                    // MQTT client has built-in reconnection, but we could add monitoring here too
                }
                catch (TaskCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Monitor error: {ex.Message}");
                }
            }
        }

        private static async Task CleanupAsync()
        {
            Console.WriteLine("\nCleaning up...");

            _batchTimer?.Dispose();

            // Publish remaining messages
            PublishBatchedMessages(null);
            await Task.Delay(500);

            if (_opcUaClient != null)
            {
                await _opcUaClient.DisconnectAsync();
                Console.WriteLine("✓ OPC UA client disconnected");
            }

            if (_mqttPublisher != null)
            {
                await _mqttPublisher.DisconnectAsync();
                Console.WriteLine("✓ MQTT publisher disconnected");
            }

            Console.WriteLine("✓ Shutdown complete");
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
        private record PendingMessage
        {
            public string Topic { get; init; } = string.Empty;
            public object Value { get; init; } = null!;
            public DateTime Timestamp { get; init; }
        }
    }
}