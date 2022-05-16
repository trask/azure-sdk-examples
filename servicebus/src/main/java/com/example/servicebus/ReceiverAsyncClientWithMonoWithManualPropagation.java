package com.example.servicebus;

import com.azure.messaging.servicebus.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ReceiverAsyncClientWithMonoWithManualPropagation {

    private static final String CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "test";

    private static final Logger logger = LoggerFactory.getLogger(ReceiverAsyncClientWithMonoWithManualPropagation.class);

    private static final Tracer tracer = GlobalOpenTelemetry.get().getTracer("com.example");

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
                .transform(MyPub::new)
                .doOnNext(message -> {
                    System.out.println("TraceId: " + Span.current().getSpanContext().getTraceId());
                    logger.info("Processing message. Session: {}, Sequence #: {}. Contents: {}\n",
                            message.getMessageId(), message.getSequenceNumber(), message.getBody());
                })
                .subscribe();

        SECONDS.sleep(10);
        logger.info("Stopping and closing the receiver");
        receiverClient.close();
    }

    private static class MyPub implements Publisher<ServiceBusReceivedMessage> {

        private final Publisher<ServiceBusReceivedMessage> delegate;

        private MyPub(Publisher<ServiceBusReceivedMessage> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void subscribe(Subscriber<? super ServiceBusReceivedMessage> subscriber) {
            delegate.subscribe(new MySubscriber(subscriber));
        }
    }

    private static class MySubscriber implements Subscriber<ServiceBusReceivedMessage> {

        private final Subscriber<? super ServiceBusReceivedMessage> delegate;

        private MySubscriber(Subscriber<? super ServiceBusReceivedMessage> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            Span span = tracer.spanBuilder("sub")
                    .setNoParent()
                    .startSpan();

            try (Scope ignored = span.makeCurrent()) {
                delegate.onSubscribe(subscription);
            } finally {
                span.end();
            }
        }

        @Override
        public void onNext(ServiceBusReceivedMessage message) {
            System.out.println("ON NEXT");
            Context context = W3CTraceContextPropagator.getInstance()
                    .extract(Context.root(), message, MessagePropertiesGetter.INSTANCE);

            Span span = tracer.spanBuilder("process message")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(context)
                    .startSpan();

            try (Scope ignored = span.makeCurrent()) {
                System.out.println("before onnext: " + Span.current().getSpanContext().getTraceId());
                delegate.onNext(message);
                System.out.println("after onnext");
            } finally {
                span.end();
            }
        }

        @Override
        public void onError(Throwable t) {
            delegate.onError(t);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }

    private static class MessagePropertiesGetter implements TextMapGetter<ServiceBusReceivedMessage> {

        private static final MessagePropertiesGetter INSTANCE = new MessagePropertiesGetter();

        @Override
        public Iterable<String> keys(ServiceBusReceivedMessage message) {
            return message.getApplicationProperties().keySet();
        }

        @Override
        public String get(ServiceBusReceivedMessage message, String key) {
            if (key.equals("traceparent")) {
                return get(message, "diagnostic-id");
            }
            return (String) message.getApplicationProperties().get(key);
        }
    }
}
