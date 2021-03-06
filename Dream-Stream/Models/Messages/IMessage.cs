﻿using Dream_Stream.Models.Messages.ConsumerMessages;
using Dream_Stream.Models.Messages.ProducerMessages;
using Dream_Stream.Models.Messages.StorageMessages;
using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [Union(0, typeof(MessageContainer))]
    [Union(1, typeof(MessageRequestResponse))]
    [Union(2, typeof(OffsetRequest))]
    [Union(3, typeof(Message))]
    [Union(4, typeof(MessageHeader))]
    [Union(5, typeof(MessageRequest))]
    [Union(6, typeof(NoNewMessage))]
    [Union(7, typeof(MessageReceived))]
    [Union(8, typeof(OffsetResponse))]
    [Union(9, typeof(StoreRequest))]
    [Union(10, typeof(ReadResponse))]
    [Union(12, typeof(StoreOffsetRequest))]
    public interface IMessage
    {
    }
}
