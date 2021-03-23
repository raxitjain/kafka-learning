package com.ernstyoung.kafkalearning.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {
        //        Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //        Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            //        Create a Producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("first_topic", "Hello World " + Integer.toString(i));
            //        Send data - asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record is sent or exception is throwm
                    if (e == null) {
                        // The record was successfully sent
                        logger.info("Recieved new metadata:\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        // Exception occurred
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
