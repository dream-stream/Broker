using System;
using System.Linq;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;
using Microsoft.AspNetCore.Mvc.Diagnostics;
using Mvccpb;

namespace Dream_Stream.Services
{
    public class TopicList
    {
        private readonly EtcdClient _client;
        public const string Prefix = "TopicList/";
        private readonly string _me;
        private LeaderElection leaderElection;


        public TopicList(EtcdClient client, string me)
        {
            _me = me;
            _client = client;
        }

        public async Task SetupTopicListWatch()
        {
            _client.WatchRange(Prefix, HandleTopicListWatch);
            var rangeResponse = await _client.GetRangeAsync(Prefix);
            HandleTopicListGet(rangeResponse);
        }

        private void HandleTopicListGet(RangeResponse rangeResponse)
        {
            foreach (var keyValue in rangeResponse.Kvs)
            {
                Task.Run(async () => await HandleElectionForKeyValue(keyValue));
            }
        }

        private async void HandleTopicListWatch(WatchResponse response)
        {
            foreach (var responseEvent in response.Events)
            {
                await HandleElectionForKeyValue(responseEvent.Kv);
            }
        }

        private async Task HandleElectionForKeyValue(KeyValue keyValue)
        {
            var topic = keyValue.Key.ToStringUtf8().Substring(Prefix.Length);

            leaderElection = new LeaderElection(_client, topic, _me);
            await leaderElection.Election();
            Console.WriteLine($"Handling Election for {keyValue.Key.ToStringUtf8()}:{keyValue.Value.ToStringUtf8()}");
        }

        public void Shutdown()
        {
            leaderElection.Shutdown();
        }

        public static async Task<int> PartitionCount(EtcdClient client, string topic)
        {
            var rangeResponseTopicList = await client.GetAsync(Prefix + topic);
            if (rangeResponseTopicList.Kvs.Count == 0)
            {
                return 0;
            }

            var wantedPartitionCountString = rangeResponseTopicList.Kvs.First().Value.ToStringUtf8();
            int.TryParse(wantedPartitionCountString, out var wantedPartitionCount);
            return wantedPartitionCount;
        }
    }
}