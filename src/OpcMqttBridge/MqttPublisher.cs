using MQTTnet;

namespace OpcMqttBridge
{
    public class MqttPublisher
    {
        private readonly string _broker;
        private readonly int _port;
        private readonly string _clientId;
        private IMqttClient? _mqttClient;

        public MqttPublisher(string broker, int port, string clientId)
        {
            _broker = broker;
            _port = port;
            _clientId = clientId;
        }
        public async Task ConnectAsync()
        {
            var factory = new MqttClientFactory();
            _mqttClient = factory.CreateMqttClient();

            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(_broker, _port)
                .WithClientId(_clientId)
                .Build();

            var response = await _mqttClient.ConnectAsync(options, CancellationToken.None);

            if (response.ResultCode != MqttClientConnectResultCode.Success)
            {
                throw new Exception($"MQTT connection failed: {response.ResultCode}");
            }
        }

        public async Task PublishAsync(string topic, string payload)
        {
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqttClient!.PublishAsync(message, CancellationToken.None);
        }

        public async Task DisconnectAsync()
        {
            if (_mqttClient != null && _mqttClient.IsConnected)
            {
                await _mqttClient.DisconnectAsync();
                _mqttClient.Dispose();
            }
        }
    }
}
