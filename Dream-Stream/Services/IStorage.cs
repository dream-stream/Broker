using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dream_Stream.Services
{
    public interface IStorage
    {
        Task<long> Store(string topic, int partition, byte[] message);
        Task<(List<byte[]> messages, int length)> Read(string consumerGroup, string topic, int partition, long offset, int amount);
        Task<long> ReadOffset(string consumerGroup, string topic, int partition);
    }
}
