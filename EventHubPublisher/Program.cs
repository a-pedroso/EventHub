using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubPublisher
{
    class Program
    {
        private const string _ehubNamespaceConnectionString = "[EVENT_HUB_NS_CONN_STR]";
        private const string _eventHubName = "[EVENT_HUB_NAME]";

        public static async Task Main(string[] args)
        {
            CancellationTokenSource _stoppingCts = new CancellationTokenSource();
            await MainPublisher(_stoppingCts.Token);
        }

        public static async Task MainPublisher(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await using var producerClient = new EventHubProducerClient(_ehubNamespaceConnectionString, _eventHubName);
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes("event " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"))));
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes("event " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"))));

                await producerClient.SendAsync(eventBatch);

                Console.WriteLine("The event batch has been published.");

                await Task.Delay(1000);
            }
        }
    }
}
