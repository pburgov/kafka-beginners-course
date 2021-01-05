package com.pburgo.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition topicPartitionToReadFrom =  new TopicPartition(TOPIC,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));
        consumer.seek(topicPartitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int messagesReadSoFar = 0;
        boolean keepOnReading = true;

        while(keepOnReading){
            ConsumerRecords<String,String > records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord r: records){
                logger.info(String.format("Key--> %s Value--> %s Partition--> [%d] Offset-->[%d]", r.key(), r.value(),
                        r.partition(), r.offset()));
                messagesReadSoFar += 1;
                if (messagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }

        }
        logger.info("Existing app!!");

    }

}
