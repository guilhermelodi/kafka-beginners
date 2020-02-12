package com.github.guilhermelodi.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        final String bootstrapServers = "localhost:9092";

        // All Producer Configs is in documentation (kafka.apache.org)
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // Create a data (producer record)
            ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", "hello world with callback " + Integer.toString(i));

            // Send data - async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or and exception is thrown
                    if (exception == null) {
                        log.info("Received new metadata,\ntopic={}\npartition={}\noffset={}\ntimestamp={}",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        log.error("Error while producing", exception);
                    }
                }
            });

            // Flush records to kafka
            producer.flush();
        }

        // Close producer
        producer.close();
    }

}
