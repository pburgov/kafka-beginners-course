package com.pburgo.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "my_ten_app";
    private static final String FIRST_TOPIC = "first_topic";

    private KafkaConsumer<String, String> consumer;
    private CountDownLatch latch;

    private static Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    public ConsumerRunnable(CountDownLatch latch) {
        this.latch = latch;
        consumer = new KafkaConsumer<String, String>(setUpProperties());
        consumer.subscribe(Collections.singleton(FIRST_TOPIC));

    }


    private Properties setUpProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }


    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(r -> {
                    logger.info(String.format("Key--> %s Value--> %s Partition--> [%d] Offset-->[%d]", r.key(), r.value(),
                            r.partition(), r.offset()));
                });
            }
        } catch (WakeupException ex) {
            logger.error("Received shutdown signal!!");
        }finally {
            consumer.close();
            latch.countDown();
        }


    }

    public void shutDown() {
        consumer.wakeup();
    }
}
