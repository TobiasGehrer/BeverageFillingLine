namespace OpcServer
{
    public class AlarmHistory
    {
        public string Parameter { get; set; }
        public double Value { get; set; }
        public DateTime Timestamp { get; set; }
        public double Target { get; set; }
        public double Deviation { get; set; }
    }
}
