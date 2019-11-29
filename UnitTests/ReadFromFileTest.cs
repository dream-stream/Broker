﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using Dream_Stream.Services;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace UnitTests
{

    public class ReadFromFileTest
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly StorageService _storage = new StorageService();

        public ReadFromFileTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        private const string FilePath = @"C:\mnt\data\Topic3";

        [Fact]
        public void ReadSpecificLineFromFileUsingLinq()
        {
            PrepareFile(1000000);
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            var line = File.ReadLines(FilePath).Skip(999999).Take(1).First();
            stopwatch.Stop();

            _testOutputHelper.WriteLine($"Time: {stopwatch.ElapsedMilliseconds}, Read: {line}");
        }

        [Fact]
        public void ReadSpecificLineFromFileWithEqualLengthOfLines()
        {
            PrepareFileWithEqualLineLength(1000000);
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            var lines = MyReadFunction(0, 20);
            stopwatch.Stop();
            
            _testOutputHelper.WriteLine($"Time: {stopwatch.ElapsedMilliseconds}, Read: {lines}");
        }

        [Fact]
        public async Task Read()
        {
            var message = LZ4MessagePackSerializer.Serialize("Bla Bla test");

            await _storage.Store("TestTopic", 3, message);

            _testOutputHelper.WriteLine("---- Iteration 1 ----");
            var (messages, offset) = await _storage.Read("MyGroup", "TestTopic", 3, 322, 40);

            messages.ForEach(item => _testOutputHelper.WriteLine($"msg: {LZ4MessagePackSerializer.Deserialize<string>(item)}"));
            _testOutputHelper.WriteLine($"offset increase: {offset}");

            _testOutputHelper.WriteLine("---- Iteration 2 ----");
            var (messages2, offset2) = await _storage.Read("MyGroup", "TestTopic", 3,  322 + offset, 40);

            messages2.ForEach(item => _testOutputHelper.WriteLine($"msg: {LZ4MessagePackSerializer.Deserialize<string>(item)}"));
            _testOutputHelper.WriteLine($"offset increase: {offset2}");
            _testOutputHelper.WriteLine($"offset total: {offset2 + offset}");
        }

        [Fact]
        public async Task GetOffset()
        {
            const string consumerGroup = "MyGroup";
            const string topic = "test";
            const int partition = 2;
            const long offsetStored = 65939485;

            await _storage.StoreOffset(consumerGroup, topic, partition, offsetStored);
            var offsetRead = await _storage.ReadOffset(consumerGroup, topic, partition);

            Assert.Equal(offsetStored, offsetRead);
        }


        [Fact]
        public async Task ReadFromPartition()
        {
            const string consumerGroup = "Anders-Is-A-Noob";
            const string topic = "Topic3";
            const int partition = 4;
            var list = new List<MessageContainer>();
            var list1 = new List<MessageContainer>();

            var (messages, length) = await _storage.Read(consumerGroup, topic, partition, 0, 6000);
            await Task.Delay(3000);
            var (messages1, length1) = await _storage.Read(consumerGroup, topic, partition, length, 6000);

            foreach (var message in messages)
            {
                list.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer); 
            }

            foreach (var message in messages1)
            {
                list1.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer);
            }

        }

        [Fact]
        public async Task ConsumePartition()
        {
            const string consumerGroup = "Anders-Is-A-Noob";
            const string topic = "Topic3";
            const int partition = 4;
            var list = new List<MessageContainer>();
            var length = 0;

            try
            {
                while (true)
                {
                    var (messages, length1) = await _storage.Read(consumerGroup, topic, partition, length, 6000);
                    length += length1;
                    if (length1 == 0) break;

                    foreach (var message in messages)
                    {
                        list.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer);
                    }
                }
            }
            catch (Exception e)
            {
                _testOutputHelper.WriteLine(e.ToString());
                throw;
            }
        }

        [Fact]
        public async Task TimerTest()
        {
            var timer = new Timer(x => { _testOutputHelper.WriteLine("Timer expired"); }, null, 100, 100);
            var timer2 = new Timer(x => { timer.Change(100, 100); }, null, 50, 50);

            await Task.Delay(1000);
        }




        private static string MyReadFunction(int offset, int amount)
        {
            var buffer = new byte[amount];

            
            File.OpenRead(FilePath).Read(buffer, offset, amount);

            var index = Array.LastIndexOf(buffer, (byte)13);

            return Encoding.UTF8.GetString(buffer, 0, index);
        }


        private static void PrepareFileWithEqualLineLength(int amountOfLines)
        {
            var content = new string[amountOfLines];

            for (var i = 0; i < amountOfLines; i++)
            {
                content[i] = $"{i + 1}";
                while (content[i].Length < amountOfLines.ToString().Length)
                {
                    content[i] += ".";
                }
            }

            File.AppendAllLines(FilePath, content);
        }

        private static void PrepareFile(int amountOfLines)
        {
            var content = new string[amountOfLines];

            for (int i = 0; i < amountOfLines; i++)
            {
                content[i] = $"{i + 1}";
            }

            File.AppendAllLines(FilePath, content);
        }
    }
}
