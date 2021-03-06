package com.pburgo.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create producer record
        for (int i = 20; i < 30; i++) {
            String message  = "Hello from java N--" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic",message);
            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata:" +
                            "Message--> " + message + "\n" +
                            "Topic--> " + metadata.topic() + System.lineSeparator() +
                            "Partition--> " + metadata.partition() + System.lineSeparator() +
                            "Offset--> " + metadata.offset() + "\n" +
                            "TimeStamp--> " + metadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
