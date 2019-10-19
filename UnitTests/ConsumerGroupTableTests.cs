using System;
using System.Collections.Generic;
using System.Text;
using Dream_Stream.Services;
using Xunit;

namespace UnitTests
{
    public class ConsumerGroupTableTests
    {

        [Fact]
        public void GetPartitionDistributionList_12Partitions2Consumers_6Each()
        {
            var list = new List<string> {"1", "2"};
            var partitionDistributionList = ConsumerGroupTable.GetPartitionDistributionList(12, list);
            foreach (var keyValuePair in partitionDistributionList) Assert.Equal(6, keyValuePair.Value.Count);
        }

        [Fact]
        public void GetPartitionDistributionList_12Partitions4Consumers_3Each()
        {
            var list = new List<string> { "1", "2", "3", "4" };
            var partitionDistributionList = ConsumerGroupTable.GetPartitionDistributionList(12, list);
            foreach (var keyValuePair in partitionDistributionList) Assert.Equal(3, keyValuePair.Value.Count);
        }

        [Fact]
        public void GetPartitionDistributionList_12Partitions5Consumers_Max3Each()
        {
            var list = new List<string> { "1", "2", "3", "4", "5"};
            var partitionDistributionList = ConsumerGroupTable.GetPartitionDistributionList(12, list);
            foreach (var keyValuePair in partitionDistributionList) {Assert.InRange(keyValuePair.Value.Count ,0 ,3);}
        }
    }
}
