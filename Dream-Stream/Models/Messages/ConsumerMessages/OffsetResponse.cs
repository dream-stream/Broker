using MessagePack;

namespace Dream_Stream.Models.Messages.ConsumerMessages
{
    [MessagePackObject]
    public class OffsetResponse : IMessage
    {
        [Key(0)] 
        public long Offset { get; set; }
    }
}
