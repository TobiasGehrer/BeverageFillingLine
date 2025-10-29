namespace OpcMqttBridge
{
    public static class DictionaryExtensions
    {
        public static object? GetValueOrDefault(this Dictionary<string, object> dict, string key)
        {
            return dict.TryGetValue(key, out var value) ? value : null;
        }
    }
}
