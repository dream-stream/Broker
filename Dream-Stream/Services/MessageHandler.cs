using Dream_Stream.Models.Messages;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Prometheus;
using System;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages.ConsumerMessages;

namespace Dream_Stream.Services
{
    public class MessageHandler
    {
        private static readonly Counter MessageBatchesReceived = Metrics.CreateCounter("message_batches_received", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private static readonly Counter MessagesReceivedSizeInBytes = Metrics.CreateCounter("messages_received_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesReceived = Metrics.CreateCounter("messages_received", "Total number of messages received.", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private readonly StorageApiService _storage = new StorageApiService();
        private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim ReadLock = new SemaphoreSlim(1, 1);

        public async Task Handle(HttpContext context, WebSocket webSocket)
        {
            ThreadPool.SetMaxThreads(10, 10);
            Console.WriteLine($"Handling message from: {context.Connection.RemoteIpAddress}");
            try
            {
                WebSocketReceiveResult result;
                do
                {
                   ThreadPool.QueueUserWorkItem(async x =>
                    {
                        var buffer = new byte[1024 * 900];
                        await ReadLock.WaitAsync();
                        result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                        ReadLock.Release();
                        var buf = buffer.Take(result.Count).ToArray();

                        if (LZ4MessagePackSerializer.Deserialize<IMessage>(buf) is MessageContainer message)
                        {

                            await HandlePublishMessage(message.Header, buf, webSocket);
                            MessageBatchesReceived.WithLabels(message.Header.Topic).Inc();
                            MessagesReceived.Inc(message.Messages.Count);
                        }
                    });
                } while (true);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.WriteLine("Connection closed");
            }
        }

        private async Task HandlePublishMessage(MessageHeader header, byte[] messages, WebSocket webSocket)
        {
            await _storage.Store(header.Topic, header.Partition, messages.Length, new MemoryStream(messages));

            //await SendResponse(new MessageReceived(), webSocket);
        }

        private static async Task SendResponse(IMessage message, WebSocket webSocket)
        {
            await Lock.WaitAsync();
            await webSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize(message)), WebSocketMessageType.Binary, true,
                CancellationToken.None);
            Lock.Release();
        }
    }
}
