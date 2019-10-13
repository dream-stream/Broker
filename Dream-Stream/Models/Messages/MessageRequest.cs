using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [MessagePackObject]
    public class MessageRequest : IMessage
    {
        [Key(1)]
        public string Topic { get; set; }
        [Key(2)]
        public int Partition { get; set; }
        [Key(3)]
        public ulong OffSet { get; set; }
        [Key(4)]
        public int ReadSize { get; set; }
    }
}