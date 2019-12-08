using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using MessagePack;
using Microsoft.Extensions.Caching.Memory;
using Prometheus;

namespace Dream_Stream.Services
{
    public class StorageApiService : IStorage
    {
        private static readonly Counter CorruptedMessagesSizeInBytes = Metrics.CreateCounter("corrupted_messages_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        //private readonly Uri _storageApiAddress = new Uri("http://localhost:5040");
        private readonly Uri _storageApiAddress = new Uri("http://storage-api");
        //private readonly Uri _storageApiAddress = new Uri("http://worker2:30050");

        private readonly HttpClient _storageClient;

        private readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions()
        {
            SizeLimit = 1000000000 //1GB
        });

        public StorageApiService()
        {
            _storageClient = new HttpClient
            {
                BaseAddress = _storageApiAddress,
                Timeout = TimeSpan.FromSeconds(10)
            };

        }
        
        public async Task<long> Store(MessageHeader header, byte[] message)
        {
            var stream = new MemoryStream(message);
            var response = await _storageClient.PostAsync($"/message?topic={header.Topic}&partition={header.Partition}&length={message.Length}", new StreamContent(stream));

            if (!response.IsSuccessStatusCode) //Retry
                response = await _storageClient.PostAsync($"/message?topic={header.Topic}&partition={header.Partition}&length={message.Length}", new ByteArrayContent(message));

            if (!long.TryParse(await response.Content.ReadAsStringAsync(), out var offset)) return 0;


            var options = new MemoryCacheEntryOptions
            {
                Size = message.Length
            };
            _cache.Set($"{header.Topic}/{header.Partition}/{offset}", message, options);

            return offset;
        }

        public async Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            //Check if the requested data is in cache.
            var cacheRead = ReadFromCache($"{topic}/{partition}", offset, amount);
            if (cacheRead.length != 0) return cacheRead;

            var request =
                WebRequest.Create(new Uri(_storageApiAddress, $"/message?consumerGroup={consumerGroup}&topic={topic}&partition={partition}&offset={offset}&amount={amount}"));
            var response = await request.GetResponseAsync();
            var header = new MessageHeader
            {
                Topic = topic,
                Partition = partition
            };

            try
            {
                var stream = response.GetResponseStream();
                var reader = new BinaryReader(stream, Encoding.UTF8);
                var buffer = reader.ReadBytes(amount);

                var (messages, length) = SplitByteRead(buffer);

                if (length == 0) return (header, null, 0);

                foreach (var message in messages)
                {
                    if (message[^1] != 67 && message[0] != 0)
                    {
                        CorruptedMessagesSizeInBytes.WithLabels($"{topic}/{partition}").Inc(buffer.Length);
                        Console.WriteLine($"Corrupted data - Topic {topic} - Partition {partition}");
                        File.WriteAllText($"/mnt/data/corrput.txt", string.Join(",", buffer));
                        (messages, length) = SplitByteRead(buffer);
                        return (header, null, 0);
                    }
                }

                return (header, messages, length);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Topic: {topic}, Partition: {partition}, Error: {e.Message}");
                Console.WriteLine(e);
                return (header, null, 0);
            }
        }

        public async Task<OffsetResponse> ReadOffset(string consumerGroup, string topic, int partition)
        {
            var endpoint = $"/message/offset?consumerGroup={consumerGroup}&topic={topic}&partition={partition}";
            var response = await _storageClient.GetAsync(endpoint);
            var offsetResponse = new OffsetResponse
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                Partition = partition,
                Offset = 0
            };

            var offset = 0L;
            if (!response.IsSuccessStatusCode && !long.TryParse(await response.Content.ReadAsStringAsync(), out offset)) return offsetResponse;

            offsetResponse.Offset = offset;
            return offsetResponse;
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

        public static (List<byte[]> messages, int length) SplitByteRead(byte[] read)
        {
            if (read.Length < 10 || read[0] == 0) return (null, 0);
            
            var list = new List<byte[]>();
            const int messageHeaderSize = 10;
            var skipLength = 0;
            var messageHeader = new byte[messageHeaderSize];

            while (true)
            {
                Array.Copy(read, skipLength, messageHeader, 0, messageHeaderSize);
                var messageLength = BitConverter.ToInt32(messageHeader);
                
                if (messageLength + skipLength + messageHeaderSize > read.Length || messageLength == 0) return (list, skipLength);

                var message = new byte[messageLength];
                Array.Copy(read, skipLength + messageHeaderSize, message, 0, messageLength);
                list.Add(message);
                skipLength += message.Length + messageHeaderSize;
                
                if (skipLength >= read.Length - messageHeaderSize) return (list, skipLength);
            }
        }
    }
}
