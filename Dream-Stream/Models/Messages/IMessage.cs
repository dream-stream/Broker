using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [Union(0, typeof(MessageContainer))]
    [Union(1, typeof(SubscriptionResponse))]
    [Union(2, typeof(SubscriptionRequest))]
    [Union(3, typeof(Message))]
    [Union(4, typeof(MessageHeader))]
    [Union(5, typeof(MessageRequest))]
    [Union(6, typeof(NoNewMessage))]
    [Union(7, typeof(MessageReceived))]
    public interface IMessage
    {
    }
}
