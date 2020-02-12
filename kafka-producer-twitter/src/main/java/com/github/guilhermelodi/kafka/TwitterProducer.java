package com.github.guilhermelodi.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    private final String consumerKey = "CSHOSW0V9oBuShlynxLd1nH7A";
    private final String consumerSecret = "Fm93y6C1QRAU2qyiwEMhURKkOgDJkpXo4x53dbjrU2j53abWsN";
    private final String token = "1223327471208226816-StLsRiAKI6LBzzmfYVzkUVdu28BdLh";
    private final String secret = "OuuVN88fcIYGjoTrdTZogsoPP49evamg1zPzQHvyTWpDt";

    List<String> terms = Lists.newArrayList("java", "kafka", "bitcoin", "super bowl", "sport", "virus");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingDeque<String> msgQueue = new LinkedBlockingDeque<>(1000);

        // Create a twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        // Create a kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // Loop to send tweets to kafka
        while (!twitterClient.isDone()) {
            String tweet = null;
            try {
                tweet = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Error in message queue pool", e);
                twitterClient.stop();
            }
            if (tweet != null) {
                log.info("Tweet: {}", tweet);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, tweet), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }
        }
        log.info("End of application");
    }

    public Client createTwitterClient(BlockingDeque<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        final String bootstrapServers = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Properties of Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // These following configs are default when idempotence config is true (but is good setting to be explicit to any reader)
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer (trade off a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32kb

        // Create producer
        return new KafkaProducer<>(properties);
    }
}
