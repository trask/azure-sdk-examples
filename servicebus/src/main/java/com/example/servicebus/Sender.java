package com.example.servicebus;

import com.azure.messaging.servicebus.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Sender {

    private static final String CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "test";

    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    public static void main(String[] args) throws InterruptedException {
        sendMessage();
        sendMessage();
        sendMessage();
        sendMessage();

        // there's a non-daemon thread "reactor-executor-1"
        System.exit(0);
    }

    private static void sendMessage() {
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildClient();

        senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
        logger.info("Sent a single message to the queue: {}", QUEUE_NAME);

        senderClient.close();
    }
}
