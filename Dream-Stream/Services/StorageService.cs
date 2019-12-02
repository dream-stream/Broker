using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using MessagePack;
using Microsoft.Extensions.Caching.Memory;

namespace Dream_Stream.Services
{
    public class StorageService : IStorage
    {
        private const string BasePath = "/mnt/data";
        private static readonly ConcurrentDictionary<string, (Timer timer, FileStream stream)> PartitionFiles = new ConcurrentDictionary<string, (Timer timer, FileStream stream)>();
        private const int TimerExecutionTimer = 10000;
        private readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions()
        {
            SizeLimit = 1000000000 //1GB
        });

        public async Task Store(MessageHeader header, byte[] message)
        {
            var path = $@"{BasePath}/{header.Topic}/{header.Partition}.txt";
            
            if (!File.Exists(path))
                CreateFile(path);
            
            if (!PartitionFiles.ContainsKey(path))
            {
                PartitionFiles.TryAdd(path, (new Timer(x =>
                    {
                        if (!PartitionFiles.TryGetValue(path, out var tuple)) return;
                        tuple.stream.Close();
                        tuple.stream.Dispose();
                        PartitionFiles.TryRemove(path, out tuple);
                        tuple.timer.Dispose();
                    }, null, TimerExecutionTimer, Timeout.Infinite),
                    new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)));
            }

            long offset = 0;
            if (PartitionFiles.TryGetValue(path, out var tuple))
            {
                tuple.timer.Change(TimerExecutionTimer, Timeout.Infinite);
                tuple.stream.Seek(0, SeekOrigin.End);
                offset = tuple.stream.Position;
                tuple.stream.Write(message);
            }

            if (!(tuple.stream is null))
            {
                var options = new MemoryCacheEntryOptions
                {
                    Size = message.Length
                };
                _cache.Set($"{path}/{offset}", message, options);
            }
        }

        public async Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            var path = $"{BasePath}/{topic}/{partition}.txt";

            var cacheItems = ReadFromCache(path, offset, amount);
            if (cacheItems.length != 0)
                return cacheItems;

            if (!File.Exists(path))
                CreateFile(path);

            if (!PartitionFiles.ContainsKey(path + consumerGroup))
                PartitionFiles.TryAdd(path + consumerGroup, (new Timer(x =>
                    {
                        if (!PartitionFiles.TryGetValue(path + consumerGroup, out var tuple)) return;
                        tuple.stream.Close();
                        tuple.stream.Dispose();
                        PartitionFiles.TryRemove(path + consumerGroup, out tuple);
                        tuple.timer.Dispose();
                    }, null, TimerExecutionTimer, Timeout.Infinite),
                    new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read)));

            var buffer = new byte[amount];
            var read = 0;
            if (PartitionFiles.TryGetValue(path + consumerGroup, out var stream))
            {
                stream.timer.Change(TimerExecutionTimer, Timeout.Infinite);
                stream.stream.Seek(offset, SeekOrigin.Begin);
                read = await stream.stream.ReadAsync(buffer, 0, amount);
            }

            if (read == 0) return (new MessageHeader() {Topic = topic, Partition = partition}, null, 0);

            var (messages, length) = SplitByteRead(buffer);

            return (new MessageHeader {Topic = topic, Partition = partition}, messages, length);
        }

        public async Task StoreOffset(string consumerGroup, string topic, int partition, long offset)
        {
            try
            {
                var path = $@"{BasePath}/offsets/{consumerGroup}/{topic}/{partition}.txt";

                if (!File.Exists(path))
                    CreateFile(path);
                if (!PartitionFiles.ContainsKey(path))
                {
                    PartitionFiles.TryAdd(path, (new Timer(x =>
                    {
                        if (!PartitionFiles.TryGetValue(path, out var tuple)) return;
                        tuple.stream.Close();
                        tuple.stream.Dispose();
                        PartitionFiles.TryRemove(path, out tuple);
                        tuple.timer.Dispose();
                    }, null, TimerExecutionTimer, Timeout.Infinite), new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)));
                }

                var data = Encoding.ASCII.GetBytes($"{offset}");

                if (PartitionFiles.TryGetValue(path, out var stream))
                {
                    stream.stream.Seek(0, SeekOrigin.Begin);
                    stream.stream.Write(data);
                    stream.timer.Change(TimerExecutionTimer, Timeout.Infinite);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }   
        }

        public async Task<OffsetResponse> ReadOffset(string consumerGroup, string topic, int partition)
        {
            try
            {
                var path = $@"{BasePath}/offsets/{consumerGroup}/{topic}/{partition}.txt";

                if (!File.Exists(path))
                    await StoreOffset(consumerGroup, topic, partition, 0);

                var buffer = new byte[8]; //long = 64 bit => 64 bit = 8 bytes
                if (PartitionFiles.TryGetValue(path, out var stream))
                {
                    stream.stream.Seek(0, SeekOrigin.Begin);
                    stream.stream.Read(buffer, 0, 8);
                    stream.timer.Change(TimerExecutionTimer, Timeout.Infinite);
                }

                return new OffsetResponse()
                {
                    ConsumerGroup = consumerGroup,
                    Topic = topic,
                    Partition = partition,
                    Offset = long.TryParse(Encoding.ASCII.GetString(buffer), out var offset) ? offset : 0
                };
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return new OffsetResponse()
                {
                    ConsumerGroup = consumerGroup,
                    Topic = topic,
                    Partition = partition,
                    Offset = 0
                };
            }
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
            if (read[0] == 0) return (null, 0);

            var list = new List<byte[]>();
            var indexOfEndMessage = 0;
            var indexOfLastNonZero = 0;
            var foundLastNonZeroIndex = false;


            for (var i = read.Count - 1; i >= 3; i--)
            {
                if (read[i] <= 10 && read[i - 1] == 0 && read[i - 2] == 0 && read[i - 3] == 201)
                {
                    indexOfEndMessage = i - 3;
                    break;
                }

                if (!foundLastNonZeroIndex && read[i] != 0)
                {
                    indexOfLastNonZero = i + 1;
                    foundLastNonZeroIndex = true;
                }
            }

            if (indexOfEndMessage == 0) indexOfEndMessage = indexOfLastNonZero;

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
                return (list, messages.Length);
            }

            return (list, indexOfEndMessage);
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
