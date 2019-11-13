using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Dream_Stream.Services
{
    public class StorageService
    {
        //private const string BasePath = "/mnt/data";
        private const string BasePath = @"C:/temp";
        private readonly Semaphore _semaphore = new Semaphore(1, 1);
        private static readonly ReaderWriterLockSlim Lock = new ReaderWriterLockSlim();

        public void Store(string topic, int partition, byte[] message)
        {
            if (!File.Exists($@"{BasePath}/{topic}/{partition}.txt"))
            {
                CreateFile(topic, partition);
            }

            Lock.EnterWriteLock();
            using var stream = new FileStream($@"{BasePath}/{topic}/{partition}.txt", FileMode.Append);
            using var writer = new BinaryWriter(stream);

            writer.Write(message);
            writer.Write((byte)10);
            writer.Close();
            stream.Close();
            Lock.ExitWriteLock();
        }

        public (List<byte[]> messages, int length) Read(string topic, int partition, long offset, int amount)
        {
            if (!File.Exists($@"{BasePath}/{topic}/{partition}.txt"))
            {
                CreateFile(topic, partition);
            }

            Lock.EnterReadLock();
            using var stream = new FileStream($@"{BasePath}/{topic}/{partition}.txt", FileMode.Open);
            stream.Seek(offset, SeekOrigin.Begin);
            using var reader = new BinaryReader(stream);

            var buffer = new byte[amount];
            reader.Read(buffer, 0, amount);
            reader.Close();
            stream.Close();
            Lock.ExitReadLock();

            return SplitByteRead(buffer);
        }

        private static (List<byte[]> messages, int length) SplitByteRead(byte[] read)
        {
            var list = new List<byte[]>();
            var indexOfEndMessage = Array.LastIndexOf(read, (byte) 10);
            var messages = read.Take(indexOfEndMessage + 1).ToArray();

            var start = 0;
            for (var i = 0; i < messages.Length; i++)
            {
                if (messages[i] != 10) continue;
                list.Add(messages.Skip(start).Take(i - start).ToArray());
                start = i+1;
            }

            return (list, start);
        }

        private void CreateFile(string topic, int partition)
        {
            Directory.CreateDirectory($@"{BasePath}/{topic}");
            var stream = File.Create($@"{BasePath}/{topic}/{partition}.txt");
            stream.Close();
        }
    }
}
