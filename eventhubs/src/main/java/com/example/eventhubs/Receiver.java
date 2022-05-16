package com.example.eventhubs;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Receiver {

    private static final String CONNECTION_STRING = System.getenv("EVENT_HUBS_CONNECTION_STRING");
    private static final String HUB_NAME = "trask5555";

    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    public static void main(String[] args) throws InterruptedException {

        EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .connectionString(CONNECTION_STRING, HUB_NAME)
                .checkpointStore(new SampleCheckpointStore())
                .processEvent(Receiver::processMessage)
                .processError(Receiver::processError)
                .buildEventProcessorClient();

        // This will start the processor. It will start processing events from all partitions.
        logger.info("Starting the processor");
        eventProcessorClient.start();
    }

    private static void processMessage(EventContext event) {
        logger.info("Processing message. Partition id: {}, Sequence #: {}",
                event.getPartitionContext().getPartitionId(), event.getEventData().getSequenceNumber());
    }

    private static void processError(ErrorContext error) {
        error.getThrowable().printStackTrace();
    }
}
