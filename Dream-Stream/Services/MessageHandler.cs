using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Prometheus;

namespace Dream_Stream.Services
{
    public class MessageHandler
    {
        private static readonly Counter MessageBatchesReceived = Metrics.CreateCounter("message_batches_received", "", new CounterConfiguration
        {
            LabelNames = new []{"Topic"}
        });
        private static readonly Counter MessagesReceived = Metrics.CreateCounter("messages_received", "Total number of messages received.");
        private static readonly BlockingCollection<MessageContainer> Messages = new BlockingCollection<MessageContainer>();
        private static StorageService _storage = new StorageService();

        public async Task Handle(HttpContext context, WebSocket webSocket)
        {
            var buffer = new byte[1024 * 4];
            WebSocketReceiveResult result = null;
            Console.WriteLine($"Handling message from: {context.Connection.RemoteIpAddress}");
            try
            {
                do
                {
                    result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    if (result.CloseStatus.HasValue) break;

                    var message =
                        LZ4MessagePackSerializer.Deserialize<IMessage>(buffer.Take(result.Count).ToArray());

                    switch (message)
                    {
                        case MessageContainer msg:
                            await HandlePublishMessage(msg.Header, buffer, webSocket);
                            MessageBatchesReceived.WithLabels(msg.Header.Topic).Inc();
                            MessagesReceived.Inc(msg.Messages.Count);
                            break;
                        case MessageRequest msg:
                            await HandleMessageRequest(msg, webSocket);
                            break;
                    }

                } while (!result.CloseStatus.HasValue);
            }
            catch (Exception e)
            {
                //Console.WriteLine(e);
                Console.WriteLine($"Connection closed");
            }
            finally
            {
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, result?.CloseStatusDescription ?? "Failed hard", CancellationToken.None);
            }
        }

        private static async Task HandleMessageRequest(MessageRequest msg, WebSocket webSocket)
        {
            var (messages, length) = _storage.Read(msg.Topic, msg.Partition, msg.OffSet, msg.ReadSize);

            if (length == 0)
            {
                await SendResponse(new NoNewMessage(), webSocket);
                return;
            }

            await SendResponse(new MessageRequestResponse
            {
                Messages = messages,
                Offset = length
            }, webSocket);

            //if (Messages.Count == 0)
            //{
            //    await SendResponse(new NoNewMessage(), webSocket);
            //    return;
            //}

            //SendResponse(Messages.Take(), webSocket);
        }

        private static async Task HandlePublishMessage(MessageHeader header, byte[] messages, WebSocket webSocket)
        {
            _storage.Store(header.Topic, header.Partition, messages);
            //Messages.Add(messages);
            await SendResponse(new MessageReceived(), webSocket);
        }

        private static async Task SendResponse(IMessage message, WebSocket webSocket)
        {
            await webSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize(message)), WebSocketMessageType.Binary, false,
                CancellationToken.None);
        }
    }
}
