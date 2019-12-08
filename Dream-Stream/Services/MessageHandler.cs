using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using Dream_Stream.Models.Messages.ProducerMessages;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Prometheus;
using System;
using System.Diagnostics;
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
        private static readonly Counter MessagesReceivedSizeInBytes = Metrics.CreateCounter("messages_received_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesSentSizeInBytes = Metrics.CreateCounter("messages_sent_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesReceived = Metrics.CreateCounter("messages_received", "Total number of messages received.", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private readonly IStorage _storage;
        private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1,1);

        public MessageHandler(bool storageMethod)
        {
            if(storageMethod)
                _storage = new StorageApiService();
            else
                _storage = new StorageService();
        }

        public async Task Handle(HttpContext context, WebSocket webSocket)
        {
            var buffer = new byte[1024 * 900];
            WebSocketReceiveResult result = null;
            Console.WriteLine($"Handling message from: {context.Connection.RemoteIpAddress}");
            var tasks = new []
            {
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask,
                Task.CompletedTask
            };


            try
            {
                do
                {
                    result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    var localResult = result;
                    var localBuffer = new byte[buffer.Length];
                    buffer.CopyTo(localBuffer, 0);

                    if (localResult.CloseStatus.HasValue) break;

                    var taskIndex = -1;

                    while (taskIndex == -1)
                    {
                        for (var i = 0; i < tasks.Length; i++)
                        {
                            Console.WriteLine($"Task{i} - {tasks[i].Status}");
                            if (tasks[i].Status != TaskStatus.RanToCompletion) continue;
                            taskIndex = i;
                            break;
                        }
                    }

                    tasks[taskIndex] = Task.Run(async () =>
                    {
                        //TODO Nicklas should make this more performant by sending size from producer.
                        var buf = localBuffer.Take(localResult.Count).ToArray();
                        try
                        {
                            var message = LZ4MessagePackSerializer.Deserialize<IMessage>(buf);

                            switch (message)
                            {
                                case MessageContainer msg:
                                    await HandlePublishMessage(msg.Header, buf, webSocket);
                                    MessagesReceivedSizeInBytes.WithLabels($"{msg.Header.Topic}_{msg.Header.Partition}").Inc(buf.Length);
                                    MessageBatchesReceived.WithLabels($"{msg.Header.Topic}_{msg.Header.Partition}").Inc();
                                    MessagesReceived.WithLabels($"{msg.Header.Topic}_{msg.Header.Partition}").Inc(msg.Messages.Count);
                                    break;
                                case MessageRequest msg:
                                    await HandleMessageRequest(msg, webSocket);
                                    break;
                                case OffsetRequest msg:
                                    await HandleOffsetRequest(msg, webSocket);
                                    break;
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    });
                } while (!result.CloseStatus.HasValue);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.WriteLine("Connection closed");
            }
            finally
            {
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, result?.CloseStatusDescription ?? "Failed hard", CancellationToken.None);
            }
        }

        private async Task HandleOffsetRequest(OffsetRequest request, WebSocket webSocket)
        {
            var offsetResponse = await _storage.ReadOffset(request.ConsumerGroup, request.Topic, request.Partition);

            await SendResponse(offsetResponse, webSocket);
        }

        public async Task HandleMessageRequest(MessageRequest request, WebSocket webSocket)
        {
            //Console.WriteLine($"Getting message from {request.Topic}/{request.Partition}/{request.OffSet}");
            if (request.OffSet == -1)
                request.OffSet = (await _storage.ReadOffset(request.ConsumerGroup, request.Topic, request.Partition)).Offset;
            
            var (header, messages, length) = await _storage.Read(request.ConsumerGroup, request.Topic, request.Partition, request.OffSet, request.ReadSize);

            if (length == 0)
            {
                await SendResponse(new NoNewMessage {Header = header}, webSocket);
                return;
            }

            var messageSizeInBytes = messages.Sum(x => x.Length);
            MessagesSentSizeInBytes.WithLabels($"{request.Topic}_{request.Partition}").Inc(messageSizeInBytes);

            await SendResponse(new MessageRequestResponse
            {
                Header = header,
                Messages = messages,
                Offset = length
            }, webSocket);
        }

        private async Task HandlePublishMessage(MessageHeader header, byte[] messages, WebSocket webSocket)
        {
            await _storage.Store(header, messages);
            
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