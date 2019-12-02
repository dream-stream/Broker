using MessagePack;

namespace Dream_Stream.Models.Messages.ProducerMessages
{
    [MessagePackObject]
    public class MessageReceived : IMessage
    {
        [Key(0)]
        public long Offset { get; set; }
        [Key(1)]
        public MessageHeader Header { get; set; }
    }
}
