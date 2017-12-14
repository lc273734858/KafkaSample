using System;
using RdKafka;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kafka.Productor
{
    class Program
    {
        const string kafkaserver = "192.168.1.125:9092";
        static void Main(string[] args)
        {
            string stop = "";
            while (stop=="")
            {
                try
                {
                    //BeginConsumer();
                    using (Producer producer = new Producer(kafkaserver))
                    using (Topic topic = producer.Topic("test"))
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            string sdata = "Hello RdKafka " + i;
                            //将message转为一个 byte[]
                            byte[] data = Encoding.UTF8.GetBytes(sdata);
                            SendMessage(topic, data, sdata);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                stop=Console.ReadLine();
            }            
        }
        static private async void SendMessage(Topic topic, byte[] data, string sdata)
        {
            DeliveryReport deliveryReport = await topic.Produce(data);
            Console.WriteLine($"发送到分区：{deliveryReport.Partition}, Offset 为: {deliveryReport.Offset},data为: {sdata}");
        }
        private static void BeginConsumer()
        {
            //配置消费者组
            var config = new Config() { GroupId = "testConsumer", StatisticsInterval = new TimeSpan(1000) };
            var consumer = new EventConsumer(config, kafkaserver);

            //注册一个事件
            consumer.OnMessage += (obj, msg) =>
            {
                try
                {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            };

            //订阅一个或者多个Topic
            consumer.Subscribe(new List<string>() { "test" });
            //var message = consumer.Consume(new TimeSpan(1000));
            //var msg = message.Value.Message;
            //string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
            //Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");

            //启动
            consumer.Start();

            Console.WriteLine("Started consumer, press enter to stop consuming");
            //Console.ReadLine();
        }
    }
}

