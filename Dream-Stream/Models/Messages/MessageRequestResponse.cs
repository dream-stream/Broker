using System.Collections.Generic;
using MessagePack;

namespace Dream_Stream.Models.Messages
{
    [MessagePackObject]
    public class MessageRequestResponse : IMessage
    {
        [Key(1)]
        public int Offset { get; set; }
        [Key(2)]
        public List<byte[]> Messages { get; set; }
    }
}
