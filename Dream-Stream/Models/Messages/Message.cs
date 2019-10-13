using System;
using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [MessagePackObject]
    public class Message : IMessage
    {
        [Key(1)]
        public string Address { get; set; }
        [Key(2)]
        public string LocationDescription { get; set; }
        [Key(3)]
        public string SensorType { get; set; }
        [Key(4)]
        public double Measurement { get; set; }
        [Key(5)]
        public string Unit { get; set; }

        public void Print()
        {
            Console.WriteLine($"{nameof(Address)}: {Address}" + Environment.NewLine +
                              $"{nameof(LocationDescription)}: {LocationDescription}" + Environment.NewLine +
                              $"{nameof(SensorType)}: {SensorType}" + Environment.NewLine +
                              $"{nameof(Measurement)}: {Measurement}" + Environment.NewLine +
                              $"{nameof(Unit)}: {Unit}" + Environment.NewLine);
        }
    }
}