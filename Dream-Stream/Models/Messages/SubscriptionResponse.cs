using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [MessagePackObject]
    public class SubscriptionResponse : IMessage
    {
        [Key(1)]
        public string TestMessage { get; set; }
    }
}
