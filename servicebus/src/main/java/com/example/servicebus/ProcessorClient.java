package com.example.servicebus;

import com.azure.messaging.servicebus.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ProcessorClient {

    private static final String CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "test";

    private static final Logger logger = LoggerFactory.getLogger(ProcessorClient.class);

    public static void main(String[] args) throws InterruptedException {
        //sendMessage();
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

        senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
        logger.info("Sent a single message to the queue: {}", QUEUE_NAME);

        senderClient.close();
    }

    private static void receiveMessages() throws InterruptedException {

        ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .processor()
                .queueName(QUEUE_NAME)
                .processMessage(ProcessorClient::processMessage)
                .processError(ProcessorClient::processError)
                .buildProcessorClient();

        logger.info("Starting the processor");
        processorClient.start();

        SECONDS.sleep(10);
        logger.info("Stopping and closing the processor");
        processorClient.close();
    }

    private static void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage message = context.getMessage();
        logger.info("Processing message. Session: {}, Sequence #: {}. Contents: {}\n",
                message.getMessageId(), message.getSequenceNumber(), message.getBody());
    }

    private static void processError(ServiceBusErrorContext context) {
        context.getException().printStackTrace();
    }
}
