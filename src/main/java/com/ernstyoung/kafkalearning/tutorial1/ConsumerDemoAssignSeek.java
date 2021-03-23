package com.ernstyoung.kafkalearning.tutorial1;

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
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

    public static void main(String[] args) {

        String topic = "first_topic";

        // Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Assign and seek are mostly used to replay data or fetch a specific message
        // Assign
        long offsetToReadFrom = 15L;
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));

        // Seek
        kafkaConsumer.seek(topicPartition,  offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : records) {
                numberOfMessagesReadSoFar++;
                logger.info("\nKey: " + consumerRecord.key() + "\nValue : " + consumerRecord.value() +
                        "\nPartition: " + consumerRecord.partition() + "\nOffset : " + consumerRecord.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false; // Exit the while loop
                    break; // Exit the for loop
                }
            }
        }
        logger.info("Exiting the application!!");
    }

}
