using System;
using FWW.Kafka;
namespace Kafka.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {

            FWW.Kafka.Consumer.BeginConsumer("testconsumer", "test");
            Console.WriteLine("begin consumer!");
            Console.ReadLine();
        }
    }
}
