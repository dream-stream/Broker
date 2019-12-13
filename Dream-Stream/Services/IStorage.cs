using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;

namespace Dream_Stream.Services
{
    public interface IStorage
    {
        Task<long> Store(string topic, int partition, int length, Stream stream);
        Task<(MessageHeader header, List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount);
        Task<OffsetResponse> ReadOffset(string consumerGroup, string topic, int partition);
    }
}