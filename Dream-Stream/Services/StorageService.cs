using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace Dream_Stream.Services
{
    public class StorageService : IStorage
    {
        private const string BasePath = "/mnt/data";
        private static readonly ReaderWriterLockSlim Lock = new ReaderWriterLockSlim();
        private static readonly ReaderWriterLockSlim OffsetLock = new ReaderWriterLockSlim();
        private static readonly Dictionary<string, (Timer timer, FileStream stream)> PartitionFiles = new Dictionary<string, (Timer timer, FileStream stream)>();
        private readonly MemoryCache _cache = new MemoryCache(new MemoryCacheOptions()
        {
            SizeLimit = 1000000000 //1GB
        });

        public async Task<long> Store(string topic, int partition, byte[] message)
        {
            try
            {
                var path = $@"{BasePath}/{topic}/{partition}.txt";
                if (!File.Exists(path))
                    CreateFile(path);
                if (!PartitionFiles.ContainsKey(path))
                {
                    PartitionFiles.TryAdd(path, (new Timer(x =>
                        {
                            if (!PartitionFiles.TryGetValue(path, out var tuple)) return;
                            tuple.stream.Close();
                            tuple.stream.Dispose();
                            PartitionFiles.Remove(path);
                            tuple.timer.Dispose();
                        }, null, 10000, 10000),
                        new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)));
                }

                long offset = 0;
                Lock.EnterWriteLock();
                if (PartitionFiles.TryGetValue(path, out var tuple))
                {
                    tuple.timer.Change(10000, 10000);
                    tuple.stream.Seek(0, SeekOrigin.End);
                    offset = tuple.stream.Position;
                    tuple.stream.Write(message);
                }

                Lock.ExitWriteLock();

                if (!(tuple.stream is null))
                {
                    var options = new MemoryCacheEntryOptions()
                    {
                        Size = message.Length
                    };
                    _cache.Set($"{path}/{offset}", message, options);
                }

                return offset;
            }
            catch (Exception e)
            {
                Lock.ExitWriteLock();
                Console.WriteLine(e);
                return 0;
            }
        }

        public async Task<(List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount)
        {
            try
            {
                await StoreOffset(consumerGroup, topic, partition, offset);

                var path = $@"{BasePath}/{topic}/{partition}.txt";

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
                        PartitionFiles.Remove(path + consumerGroup);
                        tuple.timer.Dispose();
                    }, null, 10000, 10000), new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)));


                var buffer = new byte[amount];
                if (PartitionFiles.TryGetValue(path + consumerGroup, out var stream))
                {
                    stream.stream.Seek(offset, SeekOrigin.Begin);
                    stream.stream.Read(buffer, 0, amount);
                    stream.timer.Change(10000, 10000);
                }

                return SplitByteRead(buffer);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return (null, 0);
            }
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
                        PartitionFiles.Remove(path);
                        tuple.timer.Dispose();
                    }, null, 10000, 10000), new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)));
                }

                var data = Encoding.ASCII.GetBytes($"{offset}");

                OffsetLock.EnterWriteLock();
                if (PartitionFiles.TryGetValue(path, out var stream))
                {
                    stream.stream.Seek(0, SeekOrigin.Begin);
                    stream.stream.Write(data);
                    stream.timer.Change(10000, 10000);
                }
                OffsetLock.ExitWriteLock();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }   
        }

        public async Task<long> ReadOffset(string consumerGroup, string topic, int partition)
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
                    stream.timer.Change(10000, 10000);
                }

                return long.TryParse(Encoding.ASCII.GetString(buffer), out var offset) ? offset : 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return 0;
            }
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

        private void CreateFile(string path)
        {
            var directories = path.Substring(0, path.LastIndexOf("/", StringComparison.Ordinal));
            Directory.CreateDirectory(directories);
            var stream = File.Create(path);
            stream.Close();
        }
    }
}
