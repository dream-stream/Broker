using System;
using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [MessagePackObject]
    public class MessageHeader : IMessage
    {
        [Key(1)]
        public string Topic { get; set; }
        [Key(2)]
        public int Partition { get; set; }

        public void Print()
        {
            Console.WriteLine($"Header: {nameof(Topic)}: {Topic}, {nameof(Partition)}: {Partition}");
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Topic != null ? Topic.GetHashCode() : 0) * 397) ^ Partition;
            }
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as MessageHeader);
        }

        public bool Equals(MessageHeader messageHeader)
        {
            return Topic.Equals(messageHeader.Topic) && Partition.Equals(messageHeader.Partition);
        }
    }
}
