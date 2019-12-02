using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using Dream_Stream.Models.Messages.ProducerMessages;
using Dream_Stream.Models.Messages.StorageMessages;
using MessagePack;
using Microsoft.Extensions.Caching.Memory;

namespace Dream_Stream.Services
{
    public class StorageApiService : IStorage
    {
        //private const string StorageAddress = "ws://storage-api/ws";
        //private const string StorageAddress = "ws://worker6:30005/ws";
        private const string StorageAddress = "ws://localhost:5040/ws";

        private readonly Dictionary<string, ClientWebSocket> _socketDict = new Dictionary<string, ClientWebSocket>(
            new[]
        {
            new KeyValuePair<string, ClientWebSocket>("StoreSocket", new ClientWebSocket()),
            new KeyValuePair<string, ClientWebSocket>("ReadSocket", new ClientWebSocket()),
            new KeyValuePair<string, ClientWebSocket>("StoreOffsetSocket", new ClientWebSocket()),
            new KeyValuePair<string, ClientWebSocket>("ReadOffsetSocket", new ClientWebSocket())
        });

        private readonly byte[] _buffer = new byte[1024 * 6];
        private readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions()
        {
            SizeLimit = 1000000000 //1GB
        });



        public async Task Store(MessageHeader header, byte[] message)
        {
            if (_socketDict.TryGetValue("StoreSocket", out var socket))
            {
                if (socket.State != WebSocketState.Open)
                    await socket.ConnectAsync(new Uri(StorageAddress), CancellationToken.None);

                await socket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new StoreRequest
                {
                    Topic = header.Topic,
                    Partition = header.Partition,
                    Message = message
                })), WebSocketMessageType.Binary, false, CancellationToken.None);

                var webSocketReceiveResult = await socket.ReceiveAsync(new ArraySegment<byte>(_buffer), CancellationToken.None);

                if (!(LZ4MessagePackSerializer.Deserialize<IMessage>(_buffer.Take(webSocketReceiveResult.Count).ToArray())
                    is MessageReceived messageResponse)) return; //Failed

                var options = new MemoryCacheEntryOptions
                {
                    Size = message.Length
                };
                _cache.Set($"{messageResponse.Header.Topic}/{messageResponse.Header.Partition}/{messageResponse.Offset}", message, options);
            }
        }

        public async Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            if (_socketDict.TryGetValue("ReadSocket", out var socket))
            {
                if (socket.State != WebSocketState.Open)
                    await socket.ConnectAsync(new Uri(StorageAddress), CancellationToken.None);

                var cacheItems = ReadFromCache($"{topic}/{partition}/{offset}", offset, amount);
                if (cacheItems.length != 0)
                {
                    return cacheItems;
                }

                await socket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new MessageRequest
                {
                    ConsumerGroup = consumerGroup,
                    Topic = topic,
                    Partition = partition,
                    OffSet = offset,
                    ReadSize = amount
                })), WebSocketMessageType.Binary, false, CancellationToken.None);

                var webSocketReceiveResult = await socket.ReceiveAsync(new ArraySegment<byte>(_buffer), CancellationToken.None);

                if (LZ4MessagePackSerializer.Deserialize<IMessage>(_buffer.Take(webSocketReceiveResult.Count).ToArray()) is ReadResponse messageResponse)
                {
                    var (messages, length) = SplitByteRead(messageResponse.Message);

                    if (LZ4MessagePackSerializer.Deserialize<IMessage>(messages[0]) is MessageContainer messageContainer)
                        return (messageContainer.Header, messages, length);
                }

            }

            return (null, null, 0);
        }

        public async Task StoreOffset(string consumerGroup, string topic, int partition, long offset)
        {   
            if (_socketDict.TryGetValue("StoreOffsetSocket", out var socket))
            {
                if (socket.State != WebSocketState.Open)
                    await socket.ConnectAsync(new Uri(StorageAddress), CancellationToken.None);

#pragma warning disable 4014
                socket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new StoreOffsetRequest
#pragma warning restore 4014
                {
                    ConsumerGroup = consumerGroup,
                    Topic = topic,
                    Partition = partition,
                    Offset = offset
                })), WebSocketMessageType.Binary, false, CancellationToken.None);
            }
        }

        public async Task<OffsetResponse> ReadOffset(string consumerGroup, string topic, int partition)
        {
            if (_socketDict.TryGetValue("ReadOffsetSocket", out var socket))
            {
                if (socket.State != WebSocketState.Open)
                    await socket.ConnectAsync(new Uri(StorageAddress), CancellationToken.None);

                await socket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new OffsetRequest
                {
                    ConsumerGroup = consumerGroup,
                    Topic = topic,
                    Partition = partition,
                })), WebSocketMessageType.Binary, false, CancellationToken.None);

                var webSocketReceiveResult = await socket.ReceiveAsync(new ArraySegment<byte>(_buffer), CancellationToken.None);

                return LZ4MessagePackSerializer.Deserialize<IMessage>(_buffer.Take(webSocketReceiveResult.Count).ToArray())
                    is OffsetResponse messageResponse
                    ? messageResponse
                    : null;
            }

            return null;
        }

        private (MessageHeader header, List<byte[]> messages, int length) ReadFromCache(string path, long offset, int amount)
        {
            (MessageHeader header, List<byte[]> messages, int length) response = (new MessageHeader(), new List<byte[]>(), 0);
            var getHeader = true;


            while (true)
            {
                if (_cache.TryGetValue($"{path}/{offset}", out byte[] item))
                {
                    if (response.length + item.Length > amount) return response;

                    if (getHeader)
                    {
                        response.header = (LZ4MessagePackSerializer.Deserialize<IMessage>(item) as MessageContainer)?.Header;
                        getHeader = false;
                    }

                    response.messages.Add(item);
                    response.length += item.Length;
                    offset += item.Length;
                }
                else
                {
                    return response;
                }
            }
        }

        private static (List<byte[]> messages, int length) SplitByteRead(IReadOnlyList<byte> read)
        {
            var list = new List<byte[]>();
            var indexOfEndMessage = 0;

            for (var i = read.Count - 1; i >= 3; i--)
            {
                if (read[i] <= 10 && read[i - 1] == 0 && read[i - 2] == 0 && read[i - 3] == 201)
                {
                    indexOfEndMessage = i - 3;
                    break;
                }
            }

            var messages = read.Take(indexOfEndMessage).ToArray();

            var start = 0;
            for (var i = 3; i < messages.Length - 3; i++)
            {
                if (read[i] == 201 && read[i + 1] == 0 && read[i + 2] == 0 && read[i + 3] <= 10)
                {
                    list.Add(messages.Skip(start).Take(i - start).ToArray());
                    start = i;
                }
            }

            return (list, indexOfEndMessage);
        }
    }
}
