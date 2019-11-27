﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using MessagePack;

namespace Dream_Stream.Services
{
    public class StorageService
    {
        private const string BasePath = "/mnt/data";
        private static readonly ReaderWriterLockSlim Lock = new ReaderWriterLockSlim();
        private static readonly ReaderWriterLockSlim OffsetLock = new ReaderWriterLockSlim();

        public Task Store(string topic, int partition, byte[] message)
        {
            var path = $@"{BasePath}/{topic}/{partition}.txt";

            if (!File.Exists(path))
                CreateFile(path);

            Lock.EnterWriteLock();
            using var stream = new FileStream(path, FileMode.Append);
            using var writer = new BinaryWriter(stream);
            writer.Write(message);
            writer.Close();
            stream.Close();
            Lock.ExitWriteLock();

            return Task.CompletedTask;
        }

        public async Task<(List<byte[]> messages, int length)> Read(string topic, int partition, long offset, int amount)
        {
            var path = $@"{BasePath}/{topic}/{partition}.txt";

            if (!File.Exists(path))
                CreateFile(path);

            Lock.EnterReadLock();
            await using var stream = new FileStream(path, FileMode.Open);
            stream.Seek(offset, SeekOrigin.Begin);
            using var reader = new BinaryReader(stream);

            var buffer = new byte[amount];
            reader.Read(buffer, 0, amount);
            reader.Close();
            stream.Close();
            Lock.ExitReadLock();

            return SplitByteRead(buffer);
        }

        public async Task StoreOffset(string consumerGroup, string topic, int partition, long offset)
        {
            var path = $@"{BasePath}/offsets/{consumerGroup}/{topic}/{partition}.txt";

            if (!File.Exists(path))
                CreateFile(path);

            OffsetLock.EnterWriteLock();
            await using var stream = new FileStream(path, FileMode.OpenOrCreate);
            await using var writer = new BinaryWriter(stream);
            writer.Write(offset);
            writer.Close();
            stream.Close();
            OffsetLock.ExitWriteLock();
        }

        public async Task<long> ReadOffset(string consumerGroup, string topic, int partition)
        {
            var path = $@"{BasePath}/offsets/{consumerGroup}/{topic}/{partition}.txt";

            if (!File.Exists(path))
                await StoreOffset(consumerGroup, topic, partition, 0);

            Lock.EnterReadLock();
            await using var stream = new FileStream(path, FileMode.Open);
            using var reader = new BinaryReader(stream);

            var offset = reader.ReadInt64();
            reader.Close();
            stream.Close();
            Lock.ExitReadLock();


            return offset;
        }

        private static (List<byte[]> messages, int length) SplitByteRead(IReadOnlyList<byte> read)
        {
            var list = new List<byte[]>();
            var indexOfEndMessage = 0;

            for (var i = read.Count - 1; i >= 3; i--)
            {
                if (read[i] <= 10 && read[i - 1] == 0 && read[i - 2] == 0 && read[i - 3] == 201)
                {
                    indexOfEndMessage = i - 2;
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
