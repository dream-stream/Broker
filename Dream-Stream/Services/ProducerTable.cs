using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;

namespace Dream_Stream.Services
{
    public class ProducerTable
    {
        private readonly EtcdClient _client;
        private string _topic;
        private const string Prefix = "Topic/";

        public ProducerTable(EtcdClient client)
        {
            _client = client;
        }

        public void SetupWatch(string topic, int partitionCount)
        {
            _topic = topic;
            _client.WatchRange(BrokerTable.Prefix, async x => await HandleBrokersChanging(x, partitionCount));
        }

        public async Task HandleRepartitioning(string topic, int partitionCount)
        {
            Console.WriteLine($"Handling repartitioning for {topic}");
            var producerTablePrefixKey = Prefix + topic + "/";
            var brokersObject = await GetBrokersObject();
            var partitionCountPrBroker = (partitionCount + brokersObject.Count - 1) / brokersObject.Count;
            var (partitionsCorrectlyPlaced, brokerPartitionCount) = await PartitionsToRepartition(producerTablePrefixKey,
                brokersObject, partitionCountPrBroker, partitionCount);
            await Repartition(brokerPartitionCount, partitionsCorrectlyPlaced, partitionCountPrBroker, brokersObject,
                producerTablePrefixKey);
        }

        private async Task Repartition(IList<int> brokerPartitionCount, IReadOnlyList<bool> partitionsCorrectlyPlaced,
            int partitionCountPrBroker, BrokersObject brokersObject, string producerTablePrefixKey)
        {
            var brokerNumber = brokerPartitionCount.Count - 1;
            for (var partitionNumber = 0; partitionNumber < partitionsCorrectlyPlaced.Count; partitionNumber++)
            {
                // If Partition Is Not Correctly placed
                if (partitionsCorrectlyPlaced[partitionNumber]) continue;
                if (brokersObject.BrokerExistArray[brokerNumber] && brokerPartitionCount[brokerNumber] < partitionCountPrBroker)
                {
                    await UpdateBrokerForPartition(brokersObject, brokerNumber, partitionNumber, producerTablePrefixKey);
                    //Console.WriteLine($"Updated broker {brokerNumber} Partition {partitionNumber}");
                    brokerPartitionCount[brokerNumber]++;
                }
                else
                {
                    partitionNumber--;
                    brokerNumber--;
                    //Console.WriteLine($"Went to next broker {brokerNumber} Partition {partitionNumber}");
                    //if(brokerNumber < 0)
                    //    Console.WriteLine("This is bad managed to get to negative broker number and this is gonna crash");
                }
            }
        }

        private async Task UpdateBrokerForPartition(BrokersObject brokersObject, int brokerNumber, int partitionNumber, string producerTablePrefixKey)
        {
            var key = producerTablePrefixKey + partitionNumber;
            var value = brokersObject.Name + brokerNumber;
            await _client.PutAsync(key, value);
        }

        private async Task<(bool[] partitionsCorrectlyPlaced, int[] brokerPartitionCount)> PartitionsToRepartition(
            string producerTablePrefixKey, BrokersObject brokersObject, int partitionCountPrBroker,
            int partitionCount)
        {
            var rangeResponseTopic = await _client.GetRangeAsync(producerTablePrefixKey);
            var partitionsCorrectlyPlaced = new bool[partitionCount];
            var brokerPartitionCount = PopulatePartitionsToRepartition(rangeResponseTopic, producerTablePrefixKey, brokersObject, partitionCountPrBroker, ref partitionsCorrectlyPlaced);
            return (partitionsCorrectlyPlaced, brokerPartitionCount);
        }

        private static int[] PopulatePartitionsToRepartition(RangeResponse rangeResponseTopic, string producerTablePrefixKey,
            BrokersObject brokersObject, int partitionCountPrBroker, ref bool[] partitionsCorrectlyPlaced)
        {
            var brokerPartitionCount = new int[brokersObject.BrokerExistArray.Length];
            foreach (var keyValue in rangeResponseTopic.Kvs)
            {
                var partitionString = keyValue.Key.ToStringUtf8().Substring(producerTablePrefixKey.Length);

                var brokerNumberString = keyValue.Value.ToStringUtf8().Split('-').Last();
                int.TryParse(brokerNumberString, out var brokerNumber);

                // if broker is alive and it does not have more than the wanted partitions
                if (brokersObject.BrokerExistArray[brokerNumber] && brokerPartitionCount[brokerNumber] < partitionCountPrBroker)
                {
                    brokerPartitionCount[brokerNumber]++;
                    int.TryParse(partitionString, out var partition);
                    partitionsCorrectlyPlaced[partition] = true;
                }
            }

            return brokerPartitionCount;
        }

        private async Task<BrokersObject> GetBrokersObject()
        {
            var rangeResponseBroker = await _client.GetRangeAsync(BrokerTable.Prefix);

            var brokerNameWithNumber = rangeResponseBroker.Kvs.First().Key.ToStringUtf8().Substring(BrokerTable.Prefix.Length);
            var brokerName = brokerNameWithNumber.Substring(0, brokerNameWithNumber.LastIndexOf('-')+1);

            // The 2 extra is to try to give it a bit of extra space so there should be less chance to extend the array.
            var arraySize = rangeResponseBroker.Kvs.Count + 2;
            var brokerArray = new bool[arraySize];
            var count = 0;
            foreach (var keyValue in rangeResponseBroker.Kvs)
            {
                int.TryParse(keyValue.Key.ToStringUtf8().Substring(BrokerTable.Prefix.Length + brokerName.Length), out var brokerNumber);
                if (brokerNumber >= arraySize) Array.Resize(ref brokerArray, arraySize*2);
                brokerArray[brokerNumber] = true;
                count++;
            }

            return new BrokersObject { BrokerExistArray = brokerArray, Count = count, Name = brokerName };
        }



        private async Task HandleBrokersChanging(WatchResponse watchResponse, int partitionCount)
        {
            if (watchResponse.Events.Count != 0) await HandleRepartitioning(_topic, partitionCount);
        }
    }

    public class BrokersObject
    {
        public bool[] BrokerExistArray { get; set; }
        public int Count { get; set; }
        public string Name { get; set; }
    }
}