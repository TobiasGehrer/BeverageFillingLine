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

        static async Task Main(string[] args)
        {
            Console.WriteLine("Kafka TimescaleDB Bridge");
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

            // Test database connection
            await TestDatabaseConnection();

            // Start consuming
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                await ConsumeMessages(cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\nConsumption cancelled.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        static async Task TestDatabaseConnection()
        {
            try
            {
                using var conn = new NpgsqlConnection(_connectionString);
                await conn.OpenAsync();
                Console.WriteLine("Successfully connected to TimescaleDB.\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to connect to TimescaleDB: {ex.Message}");
                throw;
            }
        }

        static async Task ConsumeMessages(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaBroker,
                GroupId = _kafkaGroupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 30000,
                MaxPollIntervalMs = 300000
            };

            using var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();

            consumer.Subscribe(_kafkaTopic);
            Console.WriteLine($"Subscribed to topic: {_kafkaTopic}");
            Console.WriteLine("Waiting for messages...\n");

            var messageCount = 0;
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
                            messageCount++;

                            // Batch insert for better performance
                            if (batch.Count >= batchSize)
                            {
                                await InsertBatch(batch);
                                consumer.Commit(consumeResult);
                                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Processed {messageCount} messages (batch: {batch.Count})");
                                batch.Clear();
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Consume error: {ex.Error.Reason}");
                    }
                }
            }
            finally
            {
                // Insert remaining batch
                if (batch.Count > 0)
                {
                    await InsertBatch(batch);
                    Console.WriteLine($"Final batch inserted: {batch.Count} messages");
                }

                consumer.Close();
            }
        }

        static MetricData? ParseMessage(string jsonMessage)
        {
            try
            {
                using var doc = JsonDocument.Parse(jsonMessage);
                var root = doc.RootElement;

                return new MetricData
                {
                    Time = root.TryGetProperty("timestamp", out var ts) ? DateTime.Parse(ts.GetString()!) : DateTime.UtcNow,
                    Metric = root.GetProperty("metric").GetString()!,
                    Line = root.GetProperty("line").GetString()!,
                    Value = root.TryGetProperty("value", out var val) ? ParseValue(val) : null,
                    ProductionOrder = root.TryGetProperty("production_order", out var po) ? po.GetString() : null,
                    Article = root.TryGetProperty("article", out var art) ? art.GetString() : null,
                    MachineName = root.TryGetProperty("machine_name", out var mn) ? mn.GetString() : null,
                    Plant = root.TryGetProperty("plant", out var pl) ? pl.GetString() : null
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to parse message: {ex.Message}");
                return null;
            }
        }

        static double? ParseValue(JsonElement element)
        {
            try
            {
                return element.ValueKind switch
                {
                    JsonValueKind.Number => element.GetDouble(),
                    JsonValueKind.String => double.TryParse(element.GetString(), out var d) ? d : null,
                    _ => null
                };
            }
            catch
            {
                return null;
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
                    (time, metric, line, value, production_order, article, machine_name, plant) 
                    VALUES ";

                var parameters = new List<string>();

                for (int i = 0; i < batch.Count; i++)
                {
                    var data = batch[i];
                    parameters.Add($"(@time{i}, @metric{i}, @line{i}, @value{i}, @po{i}, @article{i}, @machine{i}, @plant{i})");

                    cmd.Parameters.AddWithValue($"@time{i}", data.Time);
                    cmd.Parameters.AddWithValue($"@metric{i}", data.Metric);
                    cmd.Parameters.AddWithValue($"@line{i}", data.Line);
                    cmd.Parameters.AddWithValue($"@value{i}", (object?)data.Value ?? DBNull.Value);
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
                Console.WriteLine($"Failed to insert batch: {ex.Message}");
            }
        }
    }
}