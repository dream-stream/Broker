using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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

        private const string FilePath = @"C:\temp\testfile.txt";

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
        public void Read()
        {
            var message = LZ4MessagePackSerializer.Serialize("Bla Bla test");

            _storage.Store("TestTopic", 3, message);

            _testOutputHelper.WriteLine("---- Iteration 1 ----");
            var response = _storage.Read("TestTopic", 3, 322, 40);

            response.messages.ForEach(item => _testOutputHelper.WriteLine($"msg: {LZ4MessagePackSerializer.Deserialize<string>(item)}"));
            _testOutputHelper.WriteLine($"offset increase: {response.length}");

            _testOutputHelper.WriteLine("---- Iteration 2 ----");
            var response1 = _storage.Read("TestTopic", 3,  322 + response.length, 40);

            response1.messages.ForEach(item => _testOutputHelper.WriteLine($"msg: {LZ4MessagePackSerializer.Deserialize<string>(item)}"));
            _testOutputHelper.WriteLine($"offset increase: {response1.length}");
            _testOutputHelper.WriteLine($"offset total: {response1.length + response.length}");
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
