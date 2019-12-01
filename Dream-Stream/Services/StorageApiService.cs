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
        private readonly ClientWebSocket _apiSocket = new ClientWebSocket();
        private readonly byte[] _buffer = new byte[1024 * 6];
        private readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions()
        {
            SizeLimit = 1000000000 //1GB
        });

        public async Task<long> Store(string topic, int partition, byte[] message)
        {
            if(_apiSocket.State != WebSocketState.Open)
                await _apiSocket.ConnectAsync(new Uri("http://Dream-Stream-Storage"), CancellationToken.None);

            await _apiSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new StoreRequest
            {
                Topic = topic,
                Partition = partition,
                Message = message
            })), WebSocketMessageType.Binary, false, CancellationToken.None);

            var webSocketReceiveResult = await _apiSocket.ReceiveAsync(new ArraySegment<byte>(_buffer), CancellationToken.None);

            if (!(LZ4MessagePackSerializer.Deserialize<IMessage>(_buffer.Take(webSocketReceiveResult.Count).ToArray()) is MessageReceived messageResponse)) return 0;

            var options = new MemoryCacheEntryOptions
            {
                Size = message.Length
            };
            _cache.Set($"{topic}/{partition}/{messageResponse.Offset}", message, options);

            return messageResponse.Offset;
        }

        public async Task<(List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            if (_apiSocket.State != WebSocketState.Open)
                await _apiSocket.ConnectAsync(new Uri("http://Dream-Stream-Storage"), CancellationToken.None);

            var cacheItems = ReadFromCache($"{topic}/{partition}/{offset}", offset, amount);
            if (cacheItems.length != 0)
            {
#pragma warning disable 4014
                _apiSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new StoreOffsetRequest
#pragma warning restore 4014
                {
                    ConsumerGroup = consumerGroup,
                    Topic = topic,
                    Partition = partition,
                    Offset = offset
                })), WebSocketMessageType.Binary, false, CancellationToken.None);
                return cacheItems;
            }

            await _apiSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new MessageRequest
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                Partition = partition,
                OffSet = offset,
                ReadSize = amount
            })), WebSocketMessageType.Binary, false, CancellationToken.None);

            var webSocketReceiveResult = await _apiSocket.ReceiveAsync(new ArraySegment<byte>(_buffer), CancellationToken.None);

            return LZ4MessagePackSerializer.Deserialize<IMessage>(_buffer.Take(webSocketReceiveResult.Count).ToArray())
                is ReadResponse messageResponse
                ? SplitByteRead(messageResponse.Message)
                : (null, 0);
        }

        public async Task<long> ReadOffset(string consumerGroup, string topic, int partition)
        {
            if (_apiSocket.State != WebSocketState.Open)
                await _apiSocket.ConnectAsync(new Uri("http://Dream-Stream-Storage"), CancellationToken.None);

            await _apiSocket.SendAsync(new ArraySegment<byte>(LZ4MessagePackSerializer.Serialize<IMessage>(new OffsetRequest
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                Partition = partition,
            })), WebSocketMessageType.Binary, false, CancellationToken.None);

            var webSocketReceiveResult = await _apiSocket.ReceiveAsync(new ArraySegment<byte>(_buffer), CancellationToken.None);
            return LZ4MessagePackSerializer.Deserialize<IMessage>(_buffer.Take(webSocketReceiveResult.Count).ToArray())
                is OffsetResponse messageResponse
                ? messageResponse.Offset
                : 0;
        }

        private (List<byte[]> messages, int length) ReadFromCache(string path, long offset, int amount)
        {
            (List<byte[]> messages, int length) response = (new List<byte[]>(), 0);

            while (true)
            {
                if (_cache.TryGetValue($"{path}/{offset}", out byte[] item))
                {
                    if (response.length + item.Length > amount) return response;

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

            if (list.Count == 0)
            {
                list.Add(messages);
            }

            return (list, start);
        }
    }
}
