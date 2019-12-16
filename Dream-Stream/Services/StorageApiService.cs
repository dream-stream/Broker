using System;
using System.Collections.Concurrent;
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
        private static readonly Gauge OffsetUpdated = Metrics.CreateGauge("offset_updated", "Last Offset Updated.", new GaugeConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private static readonly Gauge OffsetRead = Metrics.CreateGauge("offset_read", "Last Offset Received From Consumer.", new GaugeConfiguration
        {
            LabelNames = new[] { "ConsumerGroupTopicPartition" }
        });



        private static readonly ConcurrentDictionary<string, long> Offsets = new ConcurrentDictionary<string, long>();

        //private readonly Uri _storageApiAddress = new Uri("http://localhost:5040");
        private readonly Uri _storageApiAddress = new Uri("http://storage-api");
        //private readonly Uri _storageApiAddress = new Uri("http://worker2:30050");

        private readonly HttpClient _storageClient;

        private static readonly MemoryCache Cache = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = 350000000 //350MB
        });

        public StorageApiService()
        {
            _storageClient = new HttpClient
            {
                BaseAddress = _storageApiAddress,
                Timeout = Timeout.InfiniteTimeSpan
            };
        }
        
        public async Task<long> Store(string topic, int partition, int length, Stream stream)
        {
            var ms = new MemoryStream();
            await stream.CopyToAsync(ms);
            ms.Seek(0, SeekOrigin.Begin);

            var response = await _storageClient.PostAsync($"/message?topic={topic}&partition={partition}&length={length}", new StreamContent(ms));

            if (!response.IsSuccessStatusCode) //Retry
            {
                Console.WriteLine($"Retry store for topic {topic}, partition {partition}");
                ms.Seek(0, SeekOrigin.Begin);
                response = await _storageClient.PostAsync(
                    $"/message?topic={topic}&partition={partition}&length={length}",
                    new StreamContent(ms));
            }

            if (!long.TryParse(await response.Content.ReadAsStringAsync(), out var offset)) return 0;


            var options = new MemoryCacheEntryOptions
            {
                Size = length
            };
            ms.Seek(0, SeekOrigin.Begin);
            Cache.Set($"{topic}/{partition}/{offset}", ms.ToArray(), options);
            Offsets.AddOrUpdate($"{topic}/{partition}", x => offset, (x, y) => offset);
            OffsetUpdated.WithLabels($"{topic}/{partition}").Set(offset);

            return offset;
        }

        public async Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            OffsetRead.WithLabels($"{consumerGroup}/{topic}/{partition}").Set(offset);
            if (Offsets.TryGetValue($"{topic}/{partition}", out var latestOffset) && latestOffset < offset)
                return (new MessageHeader {Topic = topic, Partition = partition}, null, 0);

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

                if (length == 0)
                {
                    //Offsets.AddOrUpdate($"{topic}/{partition}", offset - 1, (key, currentValue) =>
                    //{
                    //    if(currentValue < offset)
                    //        return offset - 1;
                    //    return currentValue;
                    //});
                    return (header, null, 0);
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

            if (!response.IsSuccessStatusCode || !long.TryParse(await response.Content.ReadAsStringAsync(), out var offset)) return offsetResponse;
            
            offsetResponse.Offset = offset;
            return offsetResponse;
        }

        private (MessageHeader header, List<byte[]> messages, int length) ReadFromCache(string path, long offset, int amount)
        {
            (MessageHeader header, List<byte[]> messages, int length) response = (new MessageHeader(), new List<byte[]>(), 0);
            var getHeader = true;


            while (true)
            {
                if (Cache.TryGetValue($"{path}/{offset}", out byte[] item))
                {
                    if (response.length + item.Length + 10 > amount) return response;

                    if (getHeader)
                    {
                        response.header = (LZ4MessagePackSerializer.Deserialize<IMessage>(item) as MessageContainer)?.Header;
                        getHeader = false;
                    }

                    response.messages.Add(item);
                    response.length += item.Length + 10;
                    offset += item.Length + 10;
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
