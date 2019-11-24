using MessagePack;

namespace Dream_Stream.Models.Messages.ConsumerMessages
{
    [MessagePackObject]
    public class OffsetRequest : IMessage
    {
        [Key(0)]
        public string ConsumerGroup { get; set; }
        [Key(1)]
        public string Topic { get; set; }
        [Key(2)]
        public int Partition { get; set; }
    }
}
