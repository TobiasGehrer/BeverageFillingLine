using Confluent.Kafka;
using Npgsql;
using System.Text.Json;

namespace KafkaTimescaledbBridge
{
    class Program
    {
        private static string _kafkaBroker;
        private static string _kafkaTopic;
        private static string _kafkaGroupId;
        private static string _connectionString;
        private static int _totalProcessed = 0;
        private static DateTime _lastStatsTime = DateTime.UtcNow;

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== Kafka TimescaleDB Bridge ===");
            Console.WriteLine("Consuming metrics and storing in TimescaleDB...\n");

            // Load configuration from environment variables
            _kafkaBroker = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "localhost:19092";
            _kafkaTopic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "beverage_metrics";
            _kafkaGroupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "timescaledb-consumer-group";

            string dbHost = Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost";
            string dbPort = Environment.GetEnvironmentVariable("DB_PORT") ?? "5432";
            string dbName = Environment.GetEnvironmentVariable("DB_NAME") ?? "beverage_data";
            string dbUser = Environment.GetEnvironmentVariable("DB_USER") ?? "admin";
            string dbPassword = Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "admin123";
            _connectionString = $"Host={dbHost};Port={dbPort};Database={dbName};Username={dbUser};Password={dbPassword}";

            Console.WriteLine($"Kafka Broker: {_kafkaBroker}");
            Console.WriteLine($"Kafka Topic: {_kafkaTopic}");
            Console.WriteLine($"Database: {dbHost}:{dbPort}/{dbName}\n");

            // Test database connection with retry
            await TestDatabaseConnectionWithRetry();

            // Start consuming
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("\n\nShutdown requested...");
                cts.Cancel();
            };

            try
            {
                await ConsumeMessages(cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\nConsumption cancelled gracefully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        static async Task TestDatabaseConnectionWithRetry()
        {
            int retries = 0;
            int maxRetries = 10;
            int delaySeconds = 5;

            while (retries < maxRetries)
            {
                try
                {
                    using var conn = new NpgsqlConnection(_connectionString);
                    await conn.OpenAsync();
                    Console.WriteLine("✓ Successfully connected to TimescaleDB\n");
                    return;
                }
                catch (Exception ex)
                {
                    retries++;
                    Console.WriteLine($"✗ Failed to connect to TimescaleDB (attempt {retries}/{maxRetries}): {ex.Message}");

                    if (retries < maxRetries)
                    {
                        Console.WriteLine($"  Retrying in {delaySeconds} seconds...");
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                        delaySeconds = Math.Min(delaySeconds * 2, 60); // Exponential backoff
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        static async Task ConsumeMessages(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaBroker,
                GroupId = _kafkaGroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,  // Changed from Latest - don't lose messages on restart!
                EnableAutoCommit = false,
                SessionTimeoutMs = 30000,
                MaxPollIntervalMs = 300000
            };

            using var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Kafka error: {e.Reason}"))
                .Build();

            consumer.Subscribe(_kafkaTopic);
            Console.WriteLine($"✓ Subscribed to topic: {_kafkaTopic}");
            Console.WriteLine("✓ Waiting for messages...\n");

            var batchSize = 100;
            var batch = new List<MetricData>();

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));

                        if (consumeResult == null)
                        {
                            continue;
                        }

                        // Parse message
                        var metricData = ParseMessage(consumeResult.Message.Value);

                        if (metricData != null)
                        {
                            batch.Add(metricData);

                            // Batch insert for better performance
                            if (batch.Count >= batchSize)
                            {
                                await InsertBatchWithRetry(batch);
                                consumer.Commit(consumeResult);

                                _totalProcessed += batch.Count;
                                PrintStats(batch.Count);

                                batch.Clear();
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Consume error: {ex.Error.Reason}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Processing error: {ex.Message}");
                    }
                }
            }
            finally
            {
                // Insert remaining batch
                if (batch.Count > 0)
                {
                    await InsertBatchWithRetry(batch);
                    Console.WriteLine($"Final batch inserted: {batch.Count} messages");
                }

                consumer.Close();
                Console.WriteLine("✓ Consumer closed gracefully");
            }
        }

        static MetricData? ParseMessage(string jsonMessage)
        {
            try
            {
                using var doc = JsonDocument.Parse(jsonMessage);
                var root = doc.RootElement;

                // Parse timestamp
                DateTime timestamp;
                if (root.TryGetProperty("timestamp", out var ts))
                {
                    timestamp = DateTime.Parse(ts.GetString()!);
                }
                else
                {
                    timestamp = DateTime.UtcNow;
                }

                // Parse value - handle both numeric and string values
                double? numericValue = null;
                string? textValue = null;

                if (root.TryGetProperty("value", out var val))
                {
                    if (val.ValueKind == JsonValueKind.Number)
                    {
                        numericValue = val.GetDouble();
                    }
                    else if (val.ValueKind == JsonValueKind.String)
                    {
                        var strValue = val.GetString();
                        // Try to parse as number first
                        if (double.TryParse(strValue, out var parsed))
                        {
                            numericValue = parsed;
                        }
                        else
                        {
                            // Keep as text (for "Pass"/"Fail", "Running"/"Error", etc.)
                            textValue = strValue;
                        }
                    }
                }

                return new MetricData
                {
                    Time = timestamp,
                    Metric = root.GetProperty("metric").GetString()!,
                    Line = root.GetProperty("line").GetString()!,
                    Value = numericValue,
                    TextValue = textValue,
                    ProductionOrder = root.TryGetProperty("production_order", out var po) ? po.GetString() : null,
                    Article = root.TryGetProperty("article", out var art) ? art.GetString() : null,
                    MachineName = root.TryGetProperty("machine_name", out var mn) ? mn.GetString() : null,
                    Plant = root.TryGetProperty("plant", out var pl) ? pl.GetString() : null
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to parse message: {ex.Message}");
                Console.WriteLine($"Message: {jsonMessage.Substring(0, Math.Min(200, jsonMessage.Length))}...");
                return null;
            }
        }

        static async Task InsertBatchWithRetry(List<MetricData> batch)
        {
            int retries = 0;
            int maxRetries = 3;

            while (retries < maxRetries)
            {
                try
                {
                    await InsertBatch(batch);
                    return;
                }
                catch (Exception ex)
                {
                    retries++;
                    Console.WriteLine($"Insert failed (attempt {retries}/{maxRetries}): {ex.Message}");

                    if (retries < maxRetries)
                    {
                        await Task.Delay(1000 * retries); // 1s, 2s, 3s delays
                    }
                    else
                    {
                        Console.WriteLine($"Failed to insert batch after {maxRetries} attempts. Data may be lost!");
                        throw;
                    }
                }
            }
        }

        static async Task InsertBatch(List<MetricData> batch)
        {
            if (batch.Count == 0)
            {
                return;
            }

            try
            {
                using var conn = new NpgsqlConnection(_connectionString);
                await conn.OpenAsync();

                using var cmd = new NpgsqlCommand();
                cmd.Connection = conn;

                var sql = @"
                    INSERT INTO beverage_metrics 
                    (time, metric, line, value, text_value, production_order, article, machine_name, plant) 
                    VALUES ";

                var parameters = new List<string>();

                for (int i = 0; i < batch.Count; i++)
                {
                    var data = batch[i];
                    parameters.Add($"(@time{i}, @metric{i}, @line{i}, @value{i}, @text{i}, @po{i}, @article{i}, @machine{i}, @plant{i})");

                    cmd.Parameters.AddWithValue($"@time{i}", data.Time);
                    cmd.Parameters.AddWithValue($"@metric{i}", data.Metric);
                    cmd.Parameters.AddWithValue($"@line{i}", data.Line);
                    cmd.Parameters.AddWithValue($"@value{i}", (object?)data.Value ?? DBNull.Value);
                    cmd.Parameters.AddWithValue($"@text{i}", (object?)data.TextValue ?? DBNull.Value);
                    cmd.Parameters.AddWithValue($"@po{i}", (object?)data.ProductionOrder ?? DBNull.Value);
                    cmd.Parameters.AddWithValue($"@article{i}", (object?)data.Article ?? DBNull.Value);
                    cmd.Parameters.AddWithValue($"@machine{i}", (object?)data.MachineName ?? DBNull.Value);
                    cmd.Parameters.AddWithValue($"@plant{i}", (object?)data.Plant ?? DBNull.Value);
                }

                cmd.CommandText = sql + string.Join(", ", parameters);
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Database insert error: {ex.Message}");
                throw;
            }
        }

        static void PrintStats(int batchSize)
        {
            var elapsed = DateTime.UtcNow - _lastStatsTime;

            // Print stats every 10 seconds
            if (elapsed.TotalSeconds >= 10)
            {
                var rate = _totalProcessed / elapsed.TotalSeconds;
                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Processed {_totalProcessed} messages ({rate:F1} msg/s)");
                _totalProcessed = 0;
                _lastStatsTime = DateTime.UtcNow;
            }
        }
    }
}