﻿using System;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using Dream_Stream.Models.Messages.ProducerMessages;
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
        private readonly IStorage _storage;

        public MessageHandler(bool storageMethod)
        {
            if (storageMethod)
            {
                _storage = new StorageApiService();
            }
            else
            {
                _storage = new StorageService();
            }
        }

        public async Task Handle(HttpContext context, WebSocket webSocket)
        {
            var buffer = new byte[1024 * 6];
            WebSocketReceiveResult result = null;
            Console.WriteLine($"Handling message from: {context.Connection.RemoteIpAddress}");
            try
            {
                do
                {
                    result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    if (result.CloseStatus.HasValue) break;

                    var buf = buffer.Take(result.Count).ToArray();

                    var message =
                        LZ4MessagePackSerializer.Deserialize<IMessage>(buf);

                    switch (message)
                    {
                        case MessageContainer msg:
                            await HandlePublishMessage(msg.Header, buf, webSocket);
                            MessageBatchesReceived.WithLabels(msg.Header.Topic).Inc();
                            MessagesReceived.Inc(msg.Messages.Count);
                            break;
                        case MessageRequest msg:
                            await HandleMessageRequest(msg, webSocket);
                            break;
                        case OffsetRequest msg:
                            await HandleOffsetRequest(msg, webSocket);
                            break;
                    }

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
            var offset = await _storage.ReadOffset(request.ConsumerGroup, request.Topic, request.Partition);

            await SendResponse(new OffsetResponse { Offset = offset }, webSocket);
        }

        private async Task HandleMessageRequest(MessageRequest msg, WebSocket webSocket)
        {
            var (messages, length) = await _storage.Read(msg.ConsumerGroup, msg.Topic, msg.Partition, msg.OffSet, msg.ReadSize);

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
        }

        private async Task HandlePublishMessage(MessageHeader header, byte[] messages, WebSocket webSocket)
        {
            var retryCounter = 0;
            while (await _storage.Store(header.Topic, header.Partition, messages) == 0 && retryCounter++ < 5)
            {
                Console.WriteLine($"Retrying store message to Topic: {header.Topic}, Partition: {header.Partition}, retry count: {retryCounter}");
                if (retryCounter == 5)
                {
                    Console.WriteLine($"Failed to store message to Topic: {header.Topic}, Partition: {header.Partition}");
                }
            }
            
            await SendResponse(new MessageReceived(), webSocket);
        }

        private static async Task SendResponse(IMessage message, WebSocket webSocket)
        {
            await webSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize(message)), WebSocketMessageType.Binary, false,
                CancellationToken.None);
        }
    }
}
