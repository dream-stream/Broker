using System;
using System.Collections.Generic;
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
        private static readonly List<MessageContainer> Messages = new List<MessageContainer>();

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
                            await HandlePublishMessage(msg, webSocket);
                            MessageBatchesReceived.WithLabels(msg.Header.Topic).Inc();
                            break;
                        case SubscriptionRequest msg:
                            await HandleSubscriptionRequest(msg, webSocket);
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
            //TODO Handle MessageRequest correctly
            if (Messages.Count == 0)
            {
                await SendResponse(new NoNewMessage(), webSocket);
                return;
            }
            
            await SendResponse(Messages[0], webSocket);
            Messages.RemoveAt(0);
        }

        private static async Task HandleSubscriptionRequest(SubscriptionRequest message, WebSocket webSocket)
        {
            //TODO Handle SubRequest correctly
            Console.WriteLine($"Consumer subscribed to: {message.Topic}");
            await SendResponse(
                new SubscriptionResponse {TestMessage = $"You did it! You subscribed to {message.Topic}"}, webSocket);
        }

        private static async Task HandlePublishMessage(MessageContainer messages, WebSocket webSocket)
        {
            //TODO Store the message
            //TODO Respond to publisher that the message is received correctly
            messages.Print();
            MessagesReceived.Inc(messages.Messages.Count);
            Messages.Add(messages);
            Console.WriteLine($"Message added to list");
            //await SendResponse(new MessageReceived(), webSocket);
        }

        private static async Task SendResponse(IMessage message, WebSocket webSocket)
        {
            await webSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize(message)), WebSocketMessageType.Binary, false,
                CancellationToken.None);
        }
    }
}
