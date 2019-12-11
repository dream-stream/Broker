﻿using Dream_Stream.Models.Messages;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Prometheus;
using System;
using System.IO;
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

        private static readonly Counter MessagesReceived = Metrics.CreateCounter("messages_received", "Total number of messages received.", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private readonly StorageApiService _storage = new StorageApiService();
        private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim ReadLock = new SemaphoreSlim(1, 1);

        public async Task Handle(HttpContext context, WebSocket webSocket)
        {
            var buffer = new byte[1024 * 900];
            Console.WriteLine($"Handling message from: {context.Connection.RemoteIpAddress}");
            var tasks = new Task[10];

            try
            {
                do
                {
                    for (var i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = Task.Run(async () =>
                        {
                            await ReadLock.WaitAsync();
                            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                            ReadLock.Release();

                            var localBuffer = buffer.SubArray(0, result.Count);
                            try
                            {
                                var message = LZ4MessagePackSerializer.Deserialize<IMessage>(localBuffer);

                                switch (message)
                                {
                                    case MessageContainer msg:
                                        await HandlePublishMessage(msg.Header, localBuffer, webSocket);
                                        MessagesReceivedSizeInBytes.WithLabels($"{msg.Header.Topic}_{msg.Header.Partition}").Inc(localBuffer.Length);
                                        MessageBatchesReceived.WithLabels($"{msg.Header.Topic}_{msg.Header.Partition}").Inc();
                                        MessagesReceived.WithLabels($"{msg.Header.Topic}_{msg.Header.Partition}").Inc(msg.Messages.Count);
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                if (e.Message.Contains("corrupt"))
                                    StorageApiService.CorruptedMessagesSizeInBytes.WithLabels("Unknown").Inc(result.Count);
                                else
                                    Console.WriteLine(e);
                            }
                        });
                    }

                    await Task.WhenAll(tasks);

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
