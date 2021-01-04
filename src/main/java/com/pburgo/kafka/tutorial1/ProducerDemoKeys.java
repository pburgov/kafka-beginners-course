package com.pburgo.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create producer record
        for (int i = 0; i < 10; i++) {


            String topic = "first_topic";
            String value  = "Hello from java N--" + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);

            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata:" +
                            "Message--> " + value + "\n" +
                            "Key--> " + key + "\n" +
                            "Topic--> " + metadata.topic() + System.lineSeparator() +
                            "Partition--> " + metadata.partition() + System.lineSeparator() +
                            "Offset--> " + metadata.offset() + "\n" +
                            "TimeStamp--> " + metadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
