using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
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
            var consumerGroupDict = GetConsumerGroupDictionary(consumerGroupRangeVal, prefixKey);
            foreach (var (consumerGroup, consumerIds) in consumerGroupDict)
            {
                var partitionDistributionList = GetPartitionDistributionList(partitionCount, consumerIds);
                await UpdatePartitionsOnEtcd(partitionDistributionList, prefixKey, consumerGroup);
            }
        }

        private static Dictionary<string, List<string>> GetConsumerGroupDictionary(IDictionary<string, string> consumerGroupRangeVal, string prefixKey)
        {
            var consumerGroupDict = new Dictionary<string, List<string>>();
            foreach (var (key, _) in consumerGroupRangeVal)
            {
                var consumerGroupAndConsumerId = key.Substring(prefixKey.Length).Split('/');
                var consumerGroup = consumerGroupAndConsumerId[0];
                var consumerId = consumerGroupAndConsumerId[1];
                if (consumerGroupDict.TryGetValue(consumerGroup, out var list)) list.Add(consumerId);
                else consumerGroupDict.Add(consumerGroup, new List<string> {consumerId});
            }

            return consumerGroupDict;
        }

        private static Dictionary<string, List<int>> GetPartitionDistributionList(int partitionCount, List<string> consumerIds)
        {
            var consumerIdPartitionList = new Dictionary<string, List<int>>();
            var partitionsPrConsumer = (partitionCount + consumerIds.Count - 1) / consumerIds.Count;

            var consumerIdIndex = 0;
            var currentPartitionCount = 0;
            for (var partition = 0; partition < partitionCount; partition++)
            {
                if (currentPartitionCount++ > partitionsPrConsumer)
                {
                    consumerIdIndex++;
                    currentPartitionCount = 0;
                }

                if (consumerIdPartitionList.TryGetValue(consumerIds[consumerIdIndex], out var partitionList))
                    partitionList.Add(partition);
                else consumerIdPartitionList.Add(consumerIds[consumerIdIndex], new List<int> {partition});
            }

            return consumerIdPartitionList;
        }

        private async Task UpdatePartitionsOnEtcd(Dictionary<string, List<int>> consumerIdPartitionList, string prefixKey, string consumerGroup)
        {
            foreach (var (consumerId, partitionList) in consumerIdPartitionList)
            {
                var partitionListString = string.Join(",", partitionList);
                var key = $"{prefixKey}{consumerGroup}/{consumerId}";
                await _client.PutAsync(new PutRequest
                {
                    IgnoreLease = true,
                    Key = ByteString.CopyFromUtf8(key),
                    Value = ByteString.CopyFromUtf8(partitionListString)
                });
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