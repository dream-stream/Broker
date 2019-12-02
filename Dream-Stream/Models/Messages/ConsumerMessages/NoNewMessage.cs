using MessagePack;

namespace Dream_Stream.Models.Messages.ConsumerMessages
{
    [MessagePackObject]
    public class NoNewMessage : IMessage
    {
        [Key(0)] 
        public MessageHeader Header { get; set; }
    }
}
