package mockstagram.retriever;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class CountRetrieverService {

    private static final Logger log = LoggerFactory.getLogger(CountRetrieverService.class);

    /*
        Pseudocode:
            1. Load config
            3. Setup the Mockstagram API client
            4. Create an InfluencerIdFetcher to load IDs from Redis
            5. Single-threaded scheduling
                Load influencer IDs from Redis each time
            6. Shutdown
    */
    public static void main(String[] args) {
        
        CountRetrieverConfig config = new CountRetrieverConfig();
        log.info("=== Starting CountRetrieverService ===");
        log.info("Kafka Broker: {}, Topic: {}", config.getKafkaBroker(), config.getKafkaTopic());
        log.info("Mockstagram Base URL: {}", config.getMockstagramUrl());
        log.info("Redis Host: {}, Port: {}, Key: {}", config.getRedisHost(), config.getRedisPort(), config.getRedisInfluencerKey());
        log.info("Fetch Interval (seconds): {}", config.getFetchIntervalSeconds());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBroker());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        final Producer<String, String> producer = new KafkaProducer<>(props);

        MockstagramApiClient apiClient = new MockstagramApiClient(config.getMockstagramUrl());

        InfluencerIdFetcher idFetcher = new InfluencerIdFetcher(
                config.getRedisHost(),
                config.getRedisPort(),
                config.getRedisInfluencerKey()
        );

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(() -> {
            try {
                List<Long> influencerIds = idFetcher.loadInfluencerIds();
                if (influencerIds.isEmpty()) {
                    log.warn("No influencer IDs found in Redis. Skipping this cycle.");
                    return;
                }
                log.info("Fetching follower counts for {} influencers...", influencerIds.size());

                for (Long influencerId : influencerIds) {
                    FollowerData data = apiClient.fetchFollowerCount(influencerId);
                    if (data != null) {
                        String key = String.valueOf(data.getPk());
                        String value = data.toJson();
                        ProducerRecord<String, String> record = new ProducerRecord<>(config.getKafkaTopic(), key, value);
                        producer.send(record, (metadata, ex) -> {
                            if (ex != null) {
                                log.error("Failed to send message to Kafka", ex);
                            } else {
                                log.debug("Message sent to partition {}, offset {}", metadata.partition(), metadata.offset());
                            }
                        });
                    }
                }
            } catch (Exception e) {
                log.error("Error in scheduled fetch job", e);
            }
        }, 0, config.getFetchIntervalSeconds(), TimeUnit.SECONDS);

        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down CountRetrieverService...");
            executor.shutdownNow();
            producer.close();
        }));
    }
}
