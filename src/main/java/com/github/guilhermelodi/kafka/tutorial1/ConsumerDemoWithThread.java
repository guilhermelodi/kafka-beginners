package com.github.guilhermelodi.kafka.tutorial1;

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

public class ConsumerDemoWithThread {

    final Logger log = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    final static String bootstrapServers = "localhost:9092";
    final static String groupId = "my-sixth-app";
    final static String topic = "first-topic";

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        CountDownLatch latch = new CountDownLatch(1);

        log.info("Creating the consumer thread");
        Runnable consumerThread = new ConsumerRunnable(latch);

        // Start the thread
        Thread myThread = new Thread(consumerThread);
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Caught shudown hook");
            ((ConsumerRunnable) consumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("Application got interrupted", e);
            } finally {
                log.info("Application is closing");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }

    private class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

            // All Consumer Configs is in documentation (kafka.apache.org)
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest, latest, none

            // Create consumer
            this.consumer = new KafkaConsumer<>(properties);

            // Subscribe consumer to a topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                // Pool for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: {}, Value: {}", record.key(), record.value());
                        log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException exception) {
                log.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // Interrupt consumer.poll(). Throw WakeUpException
            consumer.wakeup();
        }
    }
}
