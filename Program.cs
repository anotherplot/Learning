using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace rmqtest
{
    class Program
    {
        static void Main(string[] args)
        {
            var adminConsumerMessages = new List<string>();
            var trackerConsumerMessages = new List<string>();
            var siteConsumerMessages = new List<string>();
            var cashierConsumerMessages = new List<string>();
            string messageToSend = "message";

            var factory = new ConnectionFactory();
            var connection = factory.CreateConnection();
            var model = connection.CreateModel();

            StartConsuming(model, trackerConsumerMessages, "tracker_queue");
            StartConsuming(model, siteConsumerMessages, "site_queue");
            StartConsuming(model, cashierConsumerMessages, "cashier_queue");
            StartConsuming(model, adminConsumerMessages, "admin_queue");

            PublishMessages("In", model);
            connection.Close();

            AssertReceivedMessages(cashierConsumerMessages, adminConsumerMessages, siteConsumerMessages,
                trackerConsumerMessages);

            void PublishMessages(string exchangeName, IModel model)
            {
                model.BasicPublish(exchangeName, "new_order", null, Encoding.UTF8.GetBytes(messageToSend));
                Thread.Sleep(500);
                model.BasicPublish(exchangeName, "order_cancelled", null, Encoding.UTF8.GetBytes(messageToSend));
                Thread.Sleep(500);
                model.BasicPublish(exchangeName, "client_verified", null, Encoding.UTF8.GetBytes(messageToSend));
                Thread.Sleep(500);

                Thread.Sleep(1000);
            }
        }

        private static void StartConsuming(IModel model, List<string> receivedMessages, string queueName)
        {
            var consumer = new EventingBasicConsumer(model);
            consumer.Received += (sender, ea) =>
            {
                var body = ea.Body.Span;
                var receivedMessage = Encoding.UTF8.GetString(body);
                receivedMessages.Add(receivedMessage);
                model.BasicAck(ea.DeliveryTag, false);
            };
            model.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
        }

        private static void AssertReceivedMessages(List<string> cashierConsumerMessages,
            List<string> adminConsumerMessages,
            List<string> siteConsumerMessages, List<string> trackerConsumerMessages)
        {
            try
            {
                if (cashierConsumerMessages.Count != 3 || adminConsumerMessages.Count != 1 ||
                    siteConsumerMessages.Count != 3 ||
                    trackerConsumerMessages.Count != 2)
                    throw new ArgumentException("Consumers received wrong number of messages");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}