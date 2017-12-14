using System;
using RdKafka;
using System.Text;
using System.Collections.Generic;

namespace FWW.Kafka
{
    public class Consumer
    {
        private static BaseConfig baseconfig;
        static Consumer()
        {
            baseconfig = new BaseConfig();
        }
        public static void BeginConsumer(string consumername,string topic)
        {
            //配置消费者组
            var config = new Config() { GroupId = consumername, StatisticsInterval = new TimeSpan(1000) };
            var consumer = new EventConsumer(config, baseconfig.KafakServer);

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
            consumer.Subscribe(new List<string>() { topic });
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
