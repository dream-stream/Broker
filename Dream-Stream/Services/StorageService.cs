using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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
    public class StorageService : IStorage
    {
        private static readonly ConcurrentDictionary<string, long> Offsets = new ConcurrentDictionary<string, long>();
        private const string BasePath = "/ssd/local";
        private static readonly SemaphoreSlim OffsetLock = new SemaphoreSlim(1, 1);
        private static readonly Counter MessagesWrittenSizeInBytes = Metrics.CreateCounter("messages_written_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesReadSizeInBytes = Metrics.CreateCounter("messages_read_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = 350000000 //350MB
        });

        public async Task<long> Store(string topic, int partition, int length, Stream stream)
        {
            var filePath = $"{BasePath}/{topic}/{partition}.txt";
            var streamKey = $"{topic}/{partition}";

            var (_lock, fileStream) = FileStreamHandler.GetFileStream(streamKey, filePath);

            if (!stream.CanSeek || !stream.CanWrite) return -1;

            var lengthInBytes = new byte[10];
            BitConverter.GetBytes(length).CopyTo(lengthInBytes, 0);

            var offset = -1L;
            await _lock.WaitAsync();
            try
            {
                fileStream.Seek(0, SeekOrigin.End);
                offset = fileStream.Position;

                await fileStream.WriteAsync(lengthInBytes);
                await stream.CopyToAsync(fileStream);
                await fileStream.FlushAsync();
                _lock.Release();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                if (offset != -1L)
                    fileStream.SetLength(offset);
                _lock.Release();
                return -1;
            }

            var options = new MemoryCacheEntryOptions
            {
                Size = length
            };
            await using var ms = new MemoryStream();
            stream.Seek(0, SeekOrigin.Begin);
            await stream.CopyToAsync(ms);
            ms.Seek(0, SeekOrigin.Begin);
            _cache.Set($"{topic}/{partition}/{offset}", ms.ToArray(), options);
            Offsets.AddOrUpdate($"{topic}/{partition}", x => offset, (x, y) => offset);

            MessagesWrittenSizeInBytes.WithLabels($"{topic}/{partition}").Inc(length);
            return offset;
        }

        public async Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            if (Offsets.TryGetValue($"{topic}/{partition}", out var latestOffset) && latestOffset < offset)
                return (new MessageHeader { Topic = topic, Partition = partition }, null, 0);

            var filePath = $"{BasePath}/{topic}/{partition}.txt";
            var streamKey = $"{consumerGroup}/{topic}/{partition}";
            
            var cacheItems = ReadFromCache(filePath, offset, amount);
            if (cacheItems.length != 0)
                return cacheItems;

            if (!File.Exists(filePath))
                CreateFile(filePath);

            var (_, stream) = FileStreamHandler.GetFileStream(streamKey, filePath);

            if (!stream.CanRead || !stream.CanSeek) throw new Exception("AArgghh Stream");
            if (!await StoreOffset(consumerGroup, topic, partition, offset)) throw new Exception("AArgghh Offset");
            var header = new MessageHeader
            {
                Topic = topic,
                Partition = partition
            };

            try
            {
                var size = (int)Math.Min(amount, stream.Length - offset);
                stream.Seek(offset, SeekOrigin.Begin);
                var reader = new BinaryReader(stream, Encoding.UTF8);
                var buffer = reader.ReadBytes(size);

                var (messages, length) = SplitByteRead(buffer);
                if (length == 0) return (header, null, 0);

                MessagesReadSizeInBytes.WithLabels($"{topic}/{partition}").Inc(size);
                return (header, messages, length);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Topic: {topic}, Partition: {partition}, Error: {e.Message}");
                Console.WriteLine(e);
                return (header, null, 0);
            }
        }

        private async Task<bool> StoreOffset(string consumerGroup, string topic, int partition, long offset, int recursiveCount = 0)
        {
            var filePath = $"{BasePath}/offsets/{consumerGroup}/{topic}/{partition}.txt";
            var streamKey = $"offset/{consumerGroup}/{topic}/{partition}";
            var (_, stream) = FileStreamHandler.GetFileStream(streamKey, filePath);

            if (!stream.CanWrite || !stream.CanRead) return false;

            await OffsetLock.WaitAsync();
            try
            {
                stream.Seek(0, SeekOrigin.Begin);
                await stream.WriteAsync(LZ4MessagePackSerializer.Serialize(offset));
                OffsetLock.Release();
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                OffsetLock.Release();

                if (recursiveCount > 2) return false;

                return await StoreOffset(consumerGroup, topic, partition, offset, ++recursiveCount);
            }
        }

        public async Task<OffsetResponse> ReadOffset(string consumerGroup, string topic, int partition)
        {
            var filePath = $"{BasePath}/offsets/{consumerGroup}/{topic}/{partition}.txt";
            var streamKey = $"offset/{consumerGroup}/{topic}/{partition}";
            var (_, stream) = FileStreamHandler.GetFileStream(streamKey, filePath);
            var offsetResponse = new OffsetResponse
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                Partition = partition,
                Offset = 0
            };

            if (!stream.CanSeek || !stream.CanRead) return offsetResponse;

            var buffer = new byte[8];
            stream.Seek(0, SeekOrigin.Begin);
            await stream.ReadAsync(buffer);

            var offset = LZ4MessagePackSerializer.Deserialize<long>(buffer);
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

        private void CreateFile(string path)
        {
            var directories = path.Substring(0, path.LastIndexOf("/", StringComparison.Ordinal));
            Directory.CreateDirectory(directories);
            var stream = File.Create(path);
            stream.Close();
        }
    }
}