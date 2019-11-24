using MessagePack;

namespace Dream_Stream.Models.Messages.ConsumerMessages
{
    [MessagePackObject]
    public class MessageRequest : IMessage
    {
        [Key(1)]
        public string Topic { get; set; }
        [Key(2)]
        public int Partition { get; set; }
        [Key(3)]
        public long OffSet { get; set; }
        [Key(4)]
        public int ReadSize { get; set; }
        [Key(5)]
        public string ConsumerGroup { get; set; }
    }
}