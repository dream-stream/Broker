using System;
using System.Collections.Generic;
using System.Linq;
using dotnet_etcd;
using Mvccpb;

namespace Dream_Stream.Services
{
    public class ConsumerGroupTable
    {
        private readonly EtcdClient _client;
        public const string Prefix = "ConsumerGroup/";

        public ConsumerGroupTable(EtcdClient client)
        {
            _client = client;
        }

        public async void HandlePartitionDistribution(string topic, int partitionCount)
        {
            var prefixKey = $"{Prefix}{topic}/";
            var consumerGroupRangeVal = await _client.GetRangeValAsync(prefixKey);
            var consumerGroupDict = new Dictionary<string, List<string>>();
            foreach (var (key, _) in consumerGroupRangeVal)
            {
                var consumerGroupAndConsumerId = key.Substring(prefixKey.Length).Split('/');
                var consumerGroup = consumerGroupAndConsumerId[0];
                var consumerId = consumerGroupAndConsumerId[1];
                if(consumerGroupDict.TryGetValue(consumerGroup, out var list)) list.Add(consumerId);
                else consumerGroupDict.Add(consumerGroup, new List<string>{consumerId});
            }

            foreach (var (consumerGroup, consumerIds) in consumerGroupDict)
            {
                var consumerIdPartitionList = new Dictionary<string, List<int>>();
                var partitionsPrConsumer = (partitionCount + consumerIds.Count - 1) / consumerIds.Count;

                var consumerIdIndex = 0;
                for (var partition = 0; partition < partitionCount; partition++)
                {
                    if (consumerIdIndex >= partitionsPrConsumer) consumerIdIndex++;

                    if (consumerIdPartitionList.TryGetValue(consumerIds[consumerIdIndex], out var partitionList)) partitionList.Add(partition);
                    else consumerIdPartitionList.Add(consumerIds[consumerIdIndex], new List<int> { partition });
                }

                foreach (var (consumerId, partitionList) in consumerIdPartitionList)
                {
                    var partitionListString = string.Join(",", partitionList);
                    await _client.PutAsync($"{prefixKey}{consumerGroup}/{consumerId}", partitionListString);
                }
            }
        }

        public void SetupWatch(string topic, int partitionCount)
        {
            var prefixKey = $"{Prefix}{topic}/";
            _client.WatchRange(prefixKey, x => HandleRedistributingOfConsumerPartitionAssignment(x, topic, partitionCount));
        }

        private void HandleRedistributingOfConsumerPartitionAssignment(WatchEvent[] watchEvents, string topic, int partitionCount)
        {
            foreach (var watchEvent in watchEvents)
            {
                switch (watchEvent.Type)
                {
                    case Event.Types.EventType.Put:
                        if (string.IsNullOrEmpty(watchEvent.Value)) HandlePartitionDistribution(topic, partitionCount);
                        break;
                    case Event.Types.EventType.Delete:
                        HandlePartitionDistribution(topic, partitionCount);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }
    }
}