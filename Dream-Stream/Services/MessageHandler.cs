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

namespace Dream_Stream.Services
{
    public class MessageHandler
    {
        private static readonly Counter MessageBatchesReceived = Metrics.CreateCounter("message_batches_received", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesReceived = Metrics.CreateCounter("messages_received", "Total number of messages received.", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private readonly IStorage _storage;
        private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim ReadLock = new SemaphoreSlim(1, 1);

        public MessageHandler(IStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public async Task Handle(HttpContext context, WebSocket webSocket)
        {
            Console.WriteLine($"Handling message from: {context.Connection.RemoteIpAddress}");
            var tasks = new Task[10];

            try
            {
                for (var i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        do
                        {
                            var buffer = new byte[1024 * 100];
                            var token = new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token;
                            token.ThrowIfCancellationRequested();
                            
                            await ReadLock.WaitAsync(token);
                            WebSocketReceiveResult result;
                            try
                            {
                                result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                                ReadLock.Release();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                                ReadLock.Release();
                                continue;
                            }

                            var buf = buffer.Take(result.Count).ToArray();

                            if (LZ4MessagePackSerializer.Deserialize<IMessage>(buf) is MessageContainer message)
                            {
                                await HandlePublishMessage(message.Header, buf, webSocket);
                                MessageBatchesReceived.WithLabels($"{message.Header.Topic}/{message.Header.Partition}").Inc();
                                MessagesReceived.WithLabels($"{message.Header.Topic}/{message.Header.Partition}").Inc(message.Messages.Count);
                            }
                        } while (webSocket != null && webSocket.State == WebSocketState.Open);
                    });
                }

                await Task.WhenAny(tasks);
                webSocket?.Dispose();
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
