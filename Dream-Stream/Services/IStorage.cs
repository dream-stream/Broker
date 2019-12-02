using System.Collections.Generic;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;

namespace Dream_Stream.Services
{
    public interface IStorage
    {
        Task Store(MessageHeader header, byte[] message);

        Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic,
            int partition, long offset, int amount);

        Task StoreOffset(string consumerGroup, string topic, int partition, long offset);

        Task<OffsetResponse> ReadOffset(string consumerGroup, string topic, int partition);
    }
}
