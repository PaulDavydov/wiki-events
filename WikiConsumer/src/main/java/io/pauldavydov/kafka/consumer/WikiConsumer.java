package io.pauldavydov.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WikiConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WikiConsumer.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Starting wiki consumer");
        String groupId = "demo-group";
        String topic = "wiki-changes";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Detected shutdown hook, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to the topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for the data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("key: {}, value: {}", record.key(), record.value());
                    logger.info("partition: {}, offset: {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Consimer is starting to shut down");
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            consumer.close();
            logger.info("Stopped wiki consumer");
        }
    }
}
