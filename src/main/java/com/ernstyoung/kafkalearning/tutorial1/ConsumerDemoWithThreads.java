package com.ernstyoung.kafkalearning.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public ConsumerDemoWithThreads() {
    }

    public void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        // Latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");

        // Create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(countDownLatch);

        // STart the thread
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.info("Application got interrupted", e);
            } finally {
                logger.info("Application has exited");
            }
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerRunnable(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            // Create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "learning-consumer-application");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // Create Consumer
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            // Subscribe consumer to our topic
            kafkaConsumer.subscribe(Arrays.asList("first_topic"));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        logger.info("\nKey: " + consumerRecord.key() + "\nValue : " + consumerRecord.value() +
                                "\nPartition: " + consumerRecord.partition() + "\nOffset : " + consumerRecord.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!!");
            } finally {
                kafkaConsumer.close();
                // Tell our main code that we are done with the consumer
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            // The wakeup() method is a special method to interrupt consumer.poll()
            // It will throw the exception WakeuoException
            kafkaConsumer.wakeup();
        }
    }

}
