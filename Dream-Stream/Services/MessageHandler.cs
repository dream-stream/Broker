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
        private static readonly Counter Counter = Metrics.CreateCounter("Messages_Received", "");

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
                            await HandlePublishMessage(msg);
                            Counter.Inc();
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
                Console.WriteLine(e);
            }
            finally
            {
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, result?.CloseStatusDescription ?? "Failed hard", CancellationToken.None);
            }
        }

        private static async Task HandleMessageRequest(MessageRequest msg, WebSocket webSocket)
        {
            //TODO Handle MessageRequest correctly
            var buffer = LZ4MessagePackSerializer.Serialize<IMessage>(new MessageContainer
            {
                Header = new MessageHeader {Topic = "SensorData", Partition = 3},
                Messages = new List<Message> { new Message {Address = "Address", LocationDescription = "Description", SensorType = "Sensor", Measurement = 20, Unit = "Unit"}}
            });
            await webSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Binary, false,
                CancellationToken.None);
        }

        private static async Task HandleSubscriptionRequest(SubscriptionRequest message, WebSocket webSocket)
        {
            //TODO Handle SubRequest correctly
            Console.WriteLine($"Consumer subscribed to: {message.Topic}");
            var buffer = LZ4MessagePackSerializer.Serialize<IMessage>(new SubscriptionResponse {TestMessage = $"You did it! You subscribed to {message.Topic}"});
            await webSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Binary, false,
                CancellationToken.None);
        }

        private static async Task HandlePublishMessage(MessageContainer messages)
        {
            //TODO Store the message
            //TODO Respond to publisher that the message is received correctly
            messages.Print();
            await Task.Run(() => Task.CompletedTask);
        }
    }
}
