using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Dream_Stream.Models.Messages;
using Dream_Stream.Models.Messages.ConsumerMessages;
using Dream_Stream.Services;
using MessagePack;
using Microsoft.AspNetCore.Mvc;
using Prometheus;

namespace Dream_Stream.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BrokerController : ControllerBase
    {
        private readonly IStorage _storage;

        private static readonly Counter MessageBatchesReceived = Metrics.CreateCounter("message_batches_received", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });
        private static readonly Counter MessagesReceivedSizeInBytes = Metrics.CreateCounter("messages_received_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesReceived = Metrics.CreateCounter("messages_received", "Total number of messages received.", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesSent = Metrics.CreateCounter("broker_messages_sent", "Total number of messages received.", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        private static readonly Counter MessagesSentSizeInBytes = Metrics.CreateCounter("messages_sent_size_in_bytes", "", new CounterConfiguration
        {
            LabelNames = new[] { "TopicPartition" }
        });

        public BrokerController(IStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> Publish([FromQuery]string topic, int partition, int length, int messageAmount)
        {
            var offset = await _storage.Store(topic, partition, length, Request.Body);
            
            MessageBatchesReceived.WithLabels($"{topic}/{partition}").Inc();
            MessagesReceivedSizeInBytes.WithLabels($"{topic}/{partition}").Inc(length);
            MessagesReceived.WithLabels($"{topic}/{partition}").Inc(messageAmount);

            return Ok(offset);
        }


        [HttpGet]
        public async Task Consume([FromQuery]string consumerGroup, string topic, int partition, long offset, int amount)
        {
            try
            {
                bool offsetReadFromFile = false;
                if (offset == -1)
                {
                    offsetReadFromFile = true;
                    offset = (await _storage.ReadOffset(consumerGroup, topic, partition)).Offset;
                }

                var (header, messages, length) = await _storage.Read(consumerGroup, topic, partition, offset, amount);

                if (length == 0)
                {
                    Response.StatusCode = 204;
                    return;
                }

                if (messages == null)
                {
                    Response.StatusCode = (int)HttpStatusCode.PartialContent;

                    var responseData2 = LZ4MessagePackSerializer.Serialize<IMessage>(new MessageRequestResponse
                    {
                        Header = header,
                        Offset = offsetReadFromFile ? (int)(offset + length) : length
                    });
                    Response.Headers.Add("Content-Length", responseData2.Length.ToString());
                    await Response.Body.WriteAsync(responseData2, 0, responseData2.Length);

                    return;
                }

                var messageSizeInBytes = messages.Sum(x => x.Length);
                MessagesSentSizeInBytes.WithLabels($"{topic}_{partition}").Inc(messageSizeInBytes);

                var responseData = LZ4MessagePackSerializer.Serialize<IMessage>(new MessageRequestResponse
                {
                    Header = header,
                    Messages = messages,
                    Offset = offsetReadFromFile ? (int)(offset + length) : length
                });

                Response.Headers.Add("Content-Length", responseData.Length.ToString());
                Response.StatusCode = 200;
                await Response.Body.WriteAsync(responseData, 0, responseData.Length);
                MessagesSent.WithLabels($"{topic}/{partition}").Inc();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

        }
    }
}