package com.example.eventhubs;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

// sender is split out separately so that a different role name can be used (APPLICATIONINSIGHTS_ROLE_NAME)
public class Sender {

    private static final String CONNECTION_STRING = System.getenv("EVENT_HUBS_CONNECTION_STRING");
    private static final String HUB_NAME = "test";

    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    public static void main(String[] args) throws InterruptedException {
        sendMessage();

        SECONDS.sleep(10);

        // there's a non-daemon thread "reactor-executor-1"
        System.exit(0);
    }

    private static void sendMessage() {
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(CONNECTION_STRING, HUB_NAME)
                .buildProducerClient();

        producer.send(singleton(new EventData("Foo")));

        logger.info("Sent a single event to the hub: {}", HUB_NAME);

        producer.close();
    }
}
