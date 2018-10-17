using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace ConfluentKafkaExample
{
    class Program
    {
        const string Servers = "localhost:9092";
        private const string Topic = "advert-topic";
        static void Main(string[] args)
        {
            if (args[0].ToLower().StartsWith("pro"))
            {
                RunProducer();
                Console.ReadLine();
                return;
            }

            RunConsumer(args[1]);
            Console.ReadLine();
        }

        private static void RunProducer()
        {
            var conf = new ProducerConfig
            {
                BootstrapServers = Servers,
            };
            var producer = new Producer<string, string>(conf);
            var val = Console.ReadLine();
            while (val != "stop")
            {
                var message = new Message<string, string>
                {
                    Value = val,
                    Key = Guid.NewGuid().ToString(),
                    Timestamp = Timestamp.Default
                };
                producer.BeginProduce(Topic, message, Handler);
                val = Console.ReadLine();
                #region Async-await
                //try
                //{
                //    var dr = await producer.ProduceAsync("my_topic", message).ConfigureAwait(false);
                //    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                //}
                //catch (KafkaException e)
                //{
                //    Console.WriteLine($"Failed:{e.Error.Reason}");
                //}
                #endregion
            }
            producer.Flush(TimeSpan.FromSeconds(10));
            producer.Dispose();
        }

        private static void Handler(DeliveryReportResult<string, string> r)
        {
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");
        }

        private static void RunConsumer(string id)
        {
            Task.Run(() => Consume("consumer1", id));
            Task.Run(() => Consume("consumer2", id));
        }

        private static void Consume(string name, string id)
        {
            var conf = new ConsumerConfig
            {
                BootstrapServers = Servers,
                AutoOffsetReset = AutoOffsetResetType.Latest,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 5000,
                GroupId = "group-" + id,
                HeartbeatIntervalMs = 3000,//default
                SessionTimeoutMs = 30000//default
            };
            var consumer = new Consumer<string, string>(conf);
            consumer.Subscribe(Topic);
            var consuming = true;
            consumer.OnError += (_, e) => { consuming = !e.IsFatal; };
            consumer.OnPartitionsAssigned += (_, partitions)
                => Console.WriteLine($"{name} Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");

            // Raised when the consumer's current assignment set has been revoked.
            consumer.OnPartitionsRevoked += (_, partitions)
                => Console.WriteLine($"{name} Revoked partitions: [{string.Join(", ", partitions)}]");

            consumer.OnPartitionEOF += (_, tpo)
                => Console.WriteLine(
                    $"{name} Reached end of topic {tpo.Topic} partition {tpo.Partition}, next message will be at offset {tpo.Offset}");

            consumer.OnStatistics += (_, json)
                => Console.WriteLine($"{name} Statistics: {json}");

            consumer.OnOffsetsCommitted += (_, e) =>
                Console.WriteLine(
                    $"{name} Offsets Commited. {e.Offsets.Select(s => s.Offset.Value.ToString()).Aggregate((b, a) => b + "," + a)}");

            while (consuming)
            {
                try
                {
                    var message = consumer.Consume();
                    Console.WriteLine($"{name} key:{message.Key} message:{message.Value} offset:{message.Offset}");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"{name} Consume Ex:{e.Error.Reason}");
                }
            }

            consumer.Close();
            consumer.Dispose();
        }
    }
}
