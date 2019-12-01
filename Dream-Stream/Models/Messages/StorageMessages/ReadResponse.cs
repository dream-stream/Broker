using MessagePack;

namespace Dream_Stream.Models.Messages.StorageMessages
{
    [MessagePackObject]
    public class ReadResponse : IMessage
    {
        [Key(0)]
        public byte[] Message { get; set; }
    }
}
