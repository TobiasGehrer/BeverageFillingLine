namespace KafkaTimescaledbBridge
{
    class MetricData
    {
        public DateTime Time { get; set; }
        public string Metric { get; set; } = string.Empty;
        public string Line { get; set; } = string.Empty;
        public double? Value { get; set; }
        public string? TextValue { get; set; }
        public string? ProductionOrder { get; set; }
        public string? Article { get; set; }
        public string? MachineName { get; set; }
        public string? Plant { get; set; }
    }
}