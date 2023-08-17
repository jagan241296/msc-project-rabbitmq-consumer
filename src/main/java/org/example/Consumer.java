package org.example;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class Consumer {

    private static final Logger logger = Logger.getLogger(Consumer.class.getName());

    private final static String RABBITMQ_EXCHANGE_NAME = "streamData.topic";
    private final static String RABBITMQ_QUEUE_NAME = "streamData.queue";

    private final static String DATADOG_LATENCY_METRIC = "rabbitmq.message.latency";

    public void consumeFromRabbitMq() {

        //********************** Connect to RabbitMQ **************************//
        var factory = createRabbitMqConnection();

        logger.info("Connecting to RabbitMQ");
        try (var connection = factory.newConnection(); var statsClient = createStatsDClient()) {

            // Create RabbitMQ Connection
            var channel = connection.createChannel();

            // Declare the topic exchange
            channel.exchangeDeclare(RABBITMQ_EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);

            // Set up the consumer
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                var props = delivery.getProperties();
                var startTime = (long) props.getHeaders().get("rabbitmq.start-time");
                var endTime = System.currentTimeMillis();
                var message = delivery.getBody();
                var latency = Math.abs(endTime - startTime);
                System.out.printf("Message Received. Total bytes: %s and latency(ms): %s\n", message.length, latency);

                // send metrics to Datadog server
                statsClient.recordDistributionValue(DATADOG_LATENCY_METRIC, latency);
            };

            CancelCallback cancelCallback = consumerTag -> {
                logger.info(() ->
                        String.format("Consumer canceled: %s", consumerTag));
            };

            // Start consuming messages
            channel.basicConsume(RABBITMQ_QUEUE_NAME, true, deliverCallback, cancelCallback);

            // Keep the consumer running by sleeping in the main thread
            logger.info("Waiting for messages. To exit press CTRL+C");
            while (true) {
                Thread.sleep(200);
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectionFactory createRabbitMqConnection() {
        var host = "20.25.42.181"; // Replace with the IP address or hostname of your remote RabbitMQ server
        int port = 5672; // Default RabbitMQ port for non-TLS connections
        String username = "rabbitmq";
        String password = "rabbitmq";
        String virtualHost = "/";

        var connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);
        return connectionFactory;
    }

    private StatsDClient createStatsDClient() {
        logger.info("Connecting to Datadog stats client");
        return new NonBlockingStatsDClientBuilder()
                .prefix("statsD")
                .hostname("localhost")
                .port(8125)
                .processorWorkers(2)
                .prefix("custom")
                .build();
    }
}
