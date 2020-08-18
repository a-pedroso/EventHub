using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System;
using System.Text;
using System.Threading.Tasks;

namespace EventHubReceiverHost
{
    class Program
    {
        private const string _ehubNamespaceConnectionString = "[EVENT_HUB_NS_CONN_STR]";
        private const string _eventHubName = "[EVENT_HUB_NAME]";
        private const string _consumerGroup = "[EVENT_HUB_CONSUMER_GROUP]";

        private const string _blobStorageConnectionString = "[BLOB_STORAGE_CONN_STR]";
        private const string _blobContainerName = "[BLOB_STORAGE_CONTAINER_NAME]";

        static async Task Main(string[] args)
        {
            await MainReceiver();
        }

        static async Task MainReceiver()
        {
            // Read from the default consumer group: $Default
            string consumerGroup = _consumerGroup ?? EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(_blobStorageConnectionString, _blobContainerName);

            // Create an event processor client to process events in the event hub
            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, _ehubNamespaceConnectionString, _eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            Console.WriteLine("start processing");
            await processor.StartProcessingAsync();

            // Wait for 10 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(300));

            // Stop the processing
            Console.WriteLine("stop processing");
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0} \n\tfrom partitionid {1}", 
                Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()),
                eventArgs.Partition.PartitionId);
            
            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
