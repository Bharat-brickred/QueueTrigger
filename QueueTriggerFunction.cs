using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using QueueTrigger.Models;


namespace QueueTrigger.Helpers
{
    public class QueueTriggerFunction
    {
        private readonly ILogger<QueueTriggerFunction> _logger;
        private readonly string _connectionString;

        public QueueTriggerFunction(ILogger<QueueTriggerFunction> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING");

        }

        [Function(nameof(QueueTriggerFunction))]
        public async Task Run(
            [ServiceBusTrigger("myqueue", Connection = "")]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation("Message ID: {id}", message.MessageId);
            _logger.LogInformation("Message Body: {body}", message.Body); 

            try
            {
                var player = JsonSerializer.Deserialize<Player>(message.Body);
                if (player != null)
                {
                    //_logger.LogInformation("Received Player: Name={Name}, Score={Score}, Team={Team}",
                    //    player.Name, player.Score, player.Team);

                    await PlayerDataHelper.SavePlayer(player, _connectionString, _logger);
                }
                else
                {
                    _logger.LogWarning("Failed to deserialize message body to Player.");
                }



                await messageActions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message ID: {id}", message.MessageId);
                await messageActions.DeadLetterMessageAsync(
        message,
        new Dictionary<string, object>
        {
            { "DeadLetterReason", "DeserializationError" },
            { "DeadLetterErrorDescription", ex.Message }
        });
            }

        }
    }
}
