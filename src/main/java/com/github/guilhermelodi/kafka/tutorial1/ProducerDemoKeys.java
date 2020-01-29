package com.github.guilhermelodi.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

        final String bootstrapServers = "localhost:9092";

        // All Producer Configs is in documentation (kafka.apache.org)
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // id-0 is going to partition 0
        // id-1 partition 0
        // id-2 partition 2
        // id-3 partition 2
        // id-4 partition 0
        // id-5 partition 0
        // id-6 partition 0
        // id-7 partition 2
        // id-8 partition 0
        // id-9 partition 2

        for (int i = 0; i < 10; i++) {
            String topic = "first-topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id-" + Integer.toString(i);

            log.info("Key: " + key);

            // Create a data (producer record)
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

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
            }).get(); // .get() make the send synchronous (don't do this in production)

            // Flush records to kafka
            producer.flush();
        }

        // Close producer
        producer.close();
    }

}
