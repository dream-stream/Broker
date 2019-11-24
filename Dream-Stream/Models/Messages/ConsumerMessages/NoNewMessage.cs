using MessagePack;

namespace Dream_Stream.Models.Messages.ConsumerMessages
{
    [MessagePackObject]
    public class NoNewMessage : IMessage
    {
    }
}
