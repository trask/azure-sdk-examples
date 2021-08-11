package com.example.servicebus;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ReceiverAsyncClientAlt {

    private static final String CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "test";

    private static final Logger logger = LoggerFactory.getLogger(ProcessorClient.class);

    public static void main(String[] args) throws InterruptedException {
        sendMessage();
        receiveMessages();

        // there's a non-daemon thread "reactor-executor-1"
        System.exit(0);
    }

    private static void sendMessage() {
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildClient();

        // send one message to the queue
        senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
        logger.info("Sent a single message to the queue: {}", QUEUE_NAME);

        senderClient.close();
    }

    private static void receiveMessages() throws InterruptedException {

        ServiceBusReceiverAsyncClient receiverClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .receiver()
                .queueName(QUEUE_NAME)
                .buildAsyncClient();

        receiverClient.receiveMessages()
                .doOnNext(message ->
                        logger.info("Processing message. Session: {}, Sequence #: {}. Contents: {}\n",
                                message.getMessageId(), message.getSequenceNumber(), message.getBody()))
                .subscribe();

        SECONDS.sleep(10);
        logger.info("Stopping and closing the receiver");
        receiverClient.close();
    }
}
