﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using MessagePack;
using Microsoft.Extensions.Caching.Memory;

namespace Dream_Stream.Services
{
    public class StorageApiService : IStorage
    {
        //private readonly Uri _storageApiAddress = new Uri("http://localhost:5040");
        private readonly Uri _storageApiAddress = new Uri("http://storage-api");

        private readonly HttpClient _storageClient = new HttpClient();

        private readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions()
        {
            SizeLimit = 1000000000 //1GB
        });

        public StorageApiService()
        {
            _storageClient.BaseAddress = _storageApiAddress;
        }
        
        public async Task<long> Store(MessageHeader header, byte[] message)
        {
            var response = await _storageClient.PostAsync($"/message?topic={header.Topic}&partition={header.Partition}&length={message.Length}", new ByteArrayContent(message));

            if (!response.IsSuccessStatusCode) return 0;

            return long.TryParse(await response.Content.ReadAsStringAsync(), out var offset) ? offset : 0;
        }

        public async Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            var endpoint = $"/message?consumerGroup={consumerGroup}&topic={topic}&partition={partition}&offset={offset}&amount={amount}";
            var response = await _storageClient.GetAsync(endpoint);
            var header = new MessageHeader
            {
                Topic = topic,
                Partition = partition
            };

            if (!response.IsSuccessStatusCode) return (header, null, 0);

            var (messages, length) = SplitByteRead(await response.Content.ReadAsByteArrayAsync());

            return (header, messages, length);
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
            if (read[0] == 0) return (null, 0);
            
            var list = new List<byte[]>();
            const int messageHeaderSize = 10;
            var length = 0;
            var skipLength = 0;
            var messageHeader = new byte[10];

            while (true)
            {

                Array.Copy(read, skipLength, messageHeader, 0, messageHeader.Length);
                var messageLength = BitConverter.ToInt32(messageHeader);
                var message = new byte[messageLength];

                if (messageLength + skipLength + messageHeaderSize > read.Length) return (list, length);

                Array.Copy(read, skipLength + messageHeaderSize, message, 0, messageLength);
                list.Add(message);
                length += message.Length;
                skipLength += message.Length + messageHeaderSize;
                
                if (skipLength == read.Length) return (list, length);
            }
        }
    }
}
