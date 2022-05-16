package com.example.servicebus;

import com.azure.messaging.servicebus.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Receiver {

    private static final String CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "test";

    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    public static void main(String[] args) throws InterruptedException {
        receiveMessages();
    }

    private static void receiveMessages() {

        ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .processor()
                .queueName(QUEUE_NAME)
                .processMessage(Receiver::processMessage)
                .processError(Receiver::processError)
                .buildProcessorClient();

        logger.info("Starting the processor");
        processorClient.start();
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
