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
            var queueName = "testQueue";
            var exchangeName = String.Empty;
            var messages = new List<string>();
            var expectedMessages = new List<string> {"The quick brown fox", "jumps over", "the lazy dog"};

            var factory = new ConnectionFactory() {HostName = "localhost"};

            var connection = factory.CreateConnection();

            var model = connection.CreateModel();
            model.QueueDeclare(queueName, true, false, false, null);
            model.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new EventingBasicConsumer(model);
            model.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            PublishMessages(exchangeName, model, queueName);

            consumer.Received += (sender, ea) =>
            {
                var body = ea.Body.Span;
                var message = Encoding.UTF8.GetString(body);
                messages.Add(message);
                Console.WriteLine($"Received: {message}");
                model.BasicAck(ea.DeliveryTag, false);
            };

            string consumerTag = model.BasicConsume(queueName, false, consumer);

            connection.Close();
            
            try
            {
                if (!expectedMessages.SequenceEqual(messages))
                    throw new ArgumentException("Received messages are not as expected");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            void PublishMessages(string exchangeName, IModel model, string queueName)
            {
                foreach (var message in expectedMessages)
                {
                    model.BasicPublish(exchangeName, queueName, null, Encoding.UTF8.GetBytes(message));
                    Thread.Sleep(500);
                }

                Thread.Sleep(1000);
            }
        }
    }
}