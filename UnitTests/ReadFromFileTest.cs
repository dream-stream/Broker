using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Services;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace UnitTests
{

    public class ReadFromFileTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

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

        //[Fact]
        //public async Task Read()
        //{
        //    var message = LZ4MessagePackSerializer.Serialize("Bla Bla test");

        //    //await _storage.Store("TestTopic", 3, message);

        //    _testOutputHelper.WriteLine("---- Iteration 1 ----");
        //    var (header, messages, offset) = await _storage.Read("MyGroup", "TestTopic", 3, 322, 40);

        //    messages.ForEach(item => _testOutputHelper.WriteLine($"msg: {LZ4MessagePackSerializer.Deserialize<string>(item)}"));
        //    _testOutputHelper.WriteLine($"offset increase: {offset}");

        //    _testOutputHelper.WriteLine("---- Iteration 2 ----");
        //    var (header2, messages2, offset2) = await _storage.Read("MyGroup", "TestTopic", 3, 322 + offset, 40);

        //    messages2.ForEach(item => _testOutputHelper.WriteLine($"msg: {LZ4MessagePackSerializer.Deserialize<string>(item)}"));
        //    _testOutputHelper.WriteLine($"offset increase: {offset2}");
        //    _testOutputHelper.WriteLine($"offset total: {offset2 + offset}");
        //}

        //[Fact]
        //public async Task GetOffset()
        //{
        //    const string consumerGroup = "MyGroup";
        //    const string topic = "test";
        //    const int partition = 2;
        //    const long offsetStored = 65939485;

        //    await _storage.StoreOffset(consumerGroup, topic, partition, offsetStored);
        //    var offsetRead = await _storage.ReadOffset(consumerGroup, topic, partition);

        //    Assert.Equal(offsetStored, offsetRead.Offset);
        //}


        //[Fact]
        //public async Task ReadFromPartition()
        //{
        //    const string consumerGroup = "Anders-Is-A-Noob";
        //    const string topic = "Topic3";
        //    const int partition = 4;
        //    var list = new List<MessageContainer>();
        //    var list1 = new List<MessageContainer>();

        //    var (header, messages, length) = await _storage.Read(consumerGroup, topic, partition, 0, 6000);
        //    await Task.Delay(3000);
        //    var (header1, messages1, length1) = await _storage.Read(consumerGroup, topic, partition, length, 6000);

        //    foreach (var message in messages)
        //    {
        //        list.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer);
        //    }

        //    foreach (var message in messages1)
        //    {
        //        list1.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer);
        //    }

        //}

        [Fact]
        public async Task ReadFromCache()
        {
            const string consumerGroup = "Anders-Is-A-Noob";
            const string topic = "Topic3";
            const int partition = 4;
            var message = LZ4MessagePackSerializer.Serialize<IMessage>(new MessageContainer()
            {
                Header = new MessageHeader
                {
                    Partition = partition,
                    Topic = topic
                },
                Messages = new List<Message>
                {
                    new Message
                    {
                        Address = "Address",
                        LocationDescription = "Second floor - bathroom",
                        Measurement = 20.34,
                        SensorType = "Temperature",
                        Unit = "C"
                    }
                }
            });
            var list = new List<MessageContainer>();


            //var offset = await _storage.Store(topic, partition, message);

            //var (messages, length) = await _storage.Read(consumerGroup, topic, partition, offset, 6000);
            //foreach (var msg in messages)
            //{
            //    list.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(msg) as MessageContainer);
            //}
        }

        [Fact]
        public async Task ConsumePartition()
        {
            const string consumerGroup = "Anders-Is-A-Noob";
            const string topic = "Topic3";
            var list = new List<MessageContainer>();
            var api = new StorageService();


            for (var i = 2; i < 3; i++)
            {
                var length = 0;
                var messagesRead = 0;
                while (true)
                {
                    var (header, messages, length1) = await api.Read(consumerGroup, topic, i, length, 1024 * 900);
                    length += length1;
                    if (length1 == 0) break;

                    foreach (var message in messages)
                    {
                        list.Add(LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer);
                        messagesRead++;
                    }
                }

                _testOutputHelper.WriteLine($"{messagesRead} batched messages read from partition {i}");
            }
        }


        [Fact]
        public async Task ConsumePartitionAsync()
        {
            const string consumerGroup = "Anders-Is-A-Noob";
            const string topic = "Topic3";
            var list = new List<MessageContainer>();
            const int partitionCount = 12;
            var messagesPerPartition = new int[partitionCount];
            var lengthPerPartition = new int[partitionCount];
            var totalMessage = 0;
            var api = new StorageApiService();

            var tasks = Enumerable.Range(0, partitionCount).Select(async i =>
            {
                var length = 0;
                var messagesRead = 0;

                while (true)
                {
                    if (messagesRead >= 80)
                    {
                        var test = 0;
                    }
                    var (header, messages, length1) = await api.Read(consumerGroup, topic, i, length, 1024 * 900);
                    length += length1;
                    lengthPerPartition[i] += length1;
                    if (length1 == 0) break;

                    foreach (var message in messages)
                    {
                        var deserialized = LZ4MessagePackSerializer.Deserialize<IMessage>(message) as MessageContainer;
                        list.Add(deserialized);
                        messagesRead++;
                        messagesPerPartition[i] += deserialized.Messages.Count;

                    }
                }

                _testOutputHelper.WriteLine($"{messagesRead} batched messages read from partition {i}");
            });

            await Task.WhenAll(tasks);

            for (var i = 0; i < partitionCount; i++)
            {
                _testOutputHelper.WriteLine($"{messagesPerPartition[i]} messages read from partition {i}");
                totalMessage += messagesPerPartition[i];
            }
            for (var i = 0; i < partitionCount; i++)
            {
                _testOutputHelper.WriteLine($"{lengthPerPartition[i]} length of partition {i}");
            }
            _testOutputHelper.WriteLine($"{totalMessage} messages read");
        }

        [Fact]
        public async Task TimerTest()
        {
            var timer = new Timer(x => { _testOutputHelper.WriteLine("Timer expired"); }, null, 100, 100);
            var timer2 = new Timer(x => { timer.Change(100, 100); }, null, 50, 50);

            await Task.Delay(1000);
        }

        [Fact]
        public void dummyTest()
        {
            var test = new byte[] {10, 20, 30};
            var test2 = test;

            test2[2] = 10;

        }

        [Fact]
        public void MessageSplit_EqualSizeAsRequested()
        {
            var message1 = new Message()
            {
                Address = "Address1",
                Measurement = 20
            };
            var message2 = new Message()
            {
                Address = "Address1",
                Measurement = 20
            };

            var data1 = LZ4MessagePackSerializer.Serialize<IMessage>(message1);
            var length1 = new byte[10];
            BitConverter.GetBytes(data1.Length).CopyTo(length1, 0);
            var data2 = LZ4MessagePackSerializer.Serialize<IMessage>(message2);
            var length2 = new byte[10];
            BitConverter.GetBytes(data2.Length).CopyTo(length2, 0);

            var dataConcat = length1.Concat(data1).Concat(length2).Concat(data2).ToArray();

            var (messages, length) = StorageApiService.SplitByteRead(dataConcat);
            Assert.Equal(dataConcat.Length, length);
        }

        [Fact]
        public void MessageSplit_RequestedSizeIsGreaterThanSizeOfMessages()
        {
            var message1 = new Message
            {
                Address = "Address1",
                Measurement = 20
            };
            var message2 = new Message
            {
                Address = "Address1",
                Measurement = 20
            };

            var data1 = LZ4MessagePackSerializer.Serialize<IMessage>(message1);
            var length1 = new byte[10];
            BitConverter.GetBytes(data1.Length).CopyTo(length1, 0);
            var data2 = LZ4MessagePackSerializer.Serialize<IMessage>(message2);
            var length2 = new byte[10];
            BitConverter.GetBytes(data2.Length).CopyTo(length2, 0);
            var appendData = new byte[20];


            var dataConcat = length1.Concat(data1).Concat(length2).Concat(data2).Concat(appendData).ToArray();

            var (messages, length) = StorageApiService.SplitByteRead(dataConcat);
            Assert.Equal(dataConcat.Length - appendData.Length, length);
        }

        [Fact]
        public void MessageSplit_RequestedSizesIsSmallerThanMessageSize()
        {
            var message1 = new Message
            {
                Address = "Address1",
                Measurement = 20
            };
            var message2 = new Message
            {
                Address = "Address1",
                Measurement = 20
            };

            var data1 = LZ4MessagePackSerializer.Serialize<IMessage>(message1);
            var length1 = new byte[10];
            BitConverter.GetBytes(data1.Length).CopyTo(length1, 0);
            var data2 = LZ4MessagePackSerializer.Serialize<IMessage>(message2);
            var length2 = new byte[10];
            BitConverter.GetBytes(data2.Length).CopyTo(length2, 0);

            var dataConcat = length1.Concat(data1).Concat(length2).Concat(data2.Take(data2.Length - 5)).ToArray(); //Message is 5 bytes to big to fit for requested amount

            var (_, length) = StorageApiService.SplitByteRead(dataConcat);
            Assert.Equal(length1.Length + data1.Length, length);

            dataConcat = length1.Concat(data1).Concat(length2).Concat(data2).Concat(length1).Concat(data1).ToArray();

            (_, length) = StorageApiService.SplitByteRead(dataConcat);
            Assert.Equal(length1.Length + data1.Length + length2.Length + data2.Length + length1.Length + data1.Length, length);
        }

        [Fact]
        public void MessageSplit_LastMessage()
        {
            var message1 = new Message
            {
                Address = "Address1",
                Measurement = 20
            };

            var data1 = LZ4MessagePackSerializer.Serialize<IMessage>(message1);
            var length1 = new byte[10];
            BitConverter.GetBytes(data1.Length).CopyTo(length1, 0);
            var appendData = new byte[100];

            var dataConcat = length1.Concat(data1).Concat(appendData).ToArray();

            var (messages, length) = StorageApiService.SplitByteRead(dataConcat);
            Assert.Equal(length1.Length + data1.Length, length);
        }

        [Fact]
        public void MessageSplit_NoMessages()
        {
           var (messages, length) = StorageApiService.SplitByteRead(new byte[0]);
            Assert.Equal(0, length);
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
