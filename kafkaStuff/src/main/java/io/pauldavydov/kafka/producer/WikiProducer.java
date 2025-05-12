package io.pauldavydov.kafka.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.background.BackgroundEventHandler;

import java.beans.EventHandler;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiProducer {
    private static final Logger logger = LoggerFactory.getLogger(WikiProducer.class);

    public static void main(String[] args) throws InterruptedException{
        logger.info("Starting WikiProducer");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "wiki-changes";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        BackgroundEventHandler eventHandler = new WikiEventHandler(topic, producer);
        EventSource.Builder eventSource = new EventSource.Builder(URI.create(url));
        BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(eventHandler, eventSource).build();
        backgroundEventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
