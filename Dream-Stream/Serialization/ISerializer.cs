using Dream_Stream.Models.Messages;

namespace Dream_Stream.Serialization
{
    public interface ISerializer
    {
        byte[] Serialize<T>(T obj);
        T Deserialize<T>(byte[] message);
    }
}
