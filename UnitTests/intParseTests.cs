using System;
using System.Diagnostics;
using Xunit;
using Xunit.Abstractions;

namespace UnitTests
{
    public class IntParseTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public IntParseTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }


        [Fact]
        public void ConvertToInt32()
        {
            // Create new stopwatch.
            var stopwatch = new Stopwatch();
            // Begin timing.
            stopwatch.Start();

            for (var i = 0; i < 10000000; i++)
            {
                var s = i.ToString();
                var int32 = Convert.ToInt32(s);
            }

            stopwatch.Stop();

            _testOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());
            

            //c# int.parse vs convert.toint32
        }

        [Fact]
        public void IntParse()
        {
            // Create new stopwatch.
            var stopwatch = new Stopwatch();
            // Begin timing.
            stopwatch.Start();

            for (var i = 0; i < 10000000; i++)
            {
                var s = i.ToString();
                var int32 = int.Parse(s);
            }

            stopwatch.Stop();

            _testOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());


            //c# int.parse vs convert.toint32
        }


        [Fact]
        public void IntTryParse()
        {
            // Create new stopwatch.
            var stopwatch = new Stopwatch();
            // Begin timing.
            stopwatch.Start();

            for (var i = 0; i < 10000000; i++)
            {
                var s = i.ToString();
                var test = 0;
                int.TryParse(s, out test);
            }

            stopwatch.Stop();

            _testOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());


            //c# int.parse vs convert.toint32
        }
    }
}
