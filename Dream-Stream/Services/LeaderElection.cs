using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Mvccpb;

namespace Dream_Stream.Services
{
    public class LeaderElection
    {
        //todo Fix time to 1
        private const int LeaseTtl = 5;
        private readonly string _leaderKey;
        private readonly string _me;
        private readonly ProducerTable _producerTable;
        private readonly string _topic;
        private bool _leader;


        public EtcdClient Client { get; set; }

        private const string Prefix = "Leader/";

        // Inspiration for the leader election have been found here: https://www.sandtable.com/etcd3-leader-election-using-python/
        public LeaderElection(EtcdClient client, string topic, string me)
        {
            Client = client;
            _topic = topic;
            _leaderKey = Prefix + topic;
            _me = me;
            _producerTable = new ProducerTable(Client);
            Client.Watch(_leaderKey, SetNewElection);
        }

        private async void SetNewElection(WatchResponse watchResponse)
        {
            if (watchResponse.Events.Any(eventS => eventS.Type == Event.Types.EventType.Delete)) await Election();
        }

        public async Task Election()
        {
            var (leader, lease) = await ElectLeader(Client, _me);
            _leader = leader;
            if (_leader)
            {
                Console.WriteLine($"I'm the leader for this leader key: {_leaderKey}");
                LeaderHandler();
                while (_leader)
                {
                    Thread.Sleep(500);
                    Client.LeaseKeepAlive(new LeaseKeepAliveRequest { ID = lease.ID }, HandleLeaseKeepAliveRequest, CancellationToken.None);
                }
            }
        }

        private async void LeaderHandler()
        {
            await _producerTable.HandleRepartitioning(_topic);
            _producerTable.SetupWatch(_topic);
        }

        private void HandleLeaseKeepAliveRequest(LeaseKeepAliveResponse response)
        {
            if (response.TTL == LeaseTtl) return;
            Console.WriteLine($"HandleLeaseKeepAliveRequest: failed to keep lease alive for leaderKey {_leaderKey}");
            _leader = false;
        }

        private async Task<(bool, LeaseGrantResponse lease)> ElectLeader(EtcdClient client, string me)
        {
            bool result;
            var lease = client.LeaseGrant(new LeaseGrantRequest { TTL = LeaseTtl });

            try
            {
                result = await AddLeader(client, _leaderKey, me, lease);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return (false, lease);
            }

            return (result, lease);
        }

        private static async Task<bool> AddLeader(EtcdClient client, string key, string value, LeaseGrantResponse lease)
        {
            var protoKey = ByteString.CopyFromUtf8(key);
            var transactionAsync = await client.TransactionAsync(new TxnRequest
            {
                Compare =
                {
                    new Compare {Key = protoKey, Version = 0}
                },
                Success =
                {
                    new RequestOp
                    {
                        RequestPut = new PutRequest
                        {
                            Key = protoKey,
                            Value = ByteString.CopyFromUtf8(value),
                            Lease = lease.ID
                        }
                    }
                }
            });
            return transactionAsync.Succeeded;
        }
    }
}