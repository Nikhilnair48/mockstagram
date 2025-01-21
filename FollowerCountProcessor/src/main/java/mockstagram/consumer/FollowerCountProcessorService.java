package mockstagram.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

public class FollowerCountProcessorService {
    private static final Logger log = LoggerFactory.getLogger(FollowerCountProcessorService.class);

    /*
        1. Create Kafka Consumer
        2. Connect to Cassandra
            Prepare statements
        3. Consume and process in a loop; Shutdown hook to close consumer gracefully
            3a) Insert timeline record
            3b) Upsert aggregator row
    */
    public static void main(String[] args) {
        FollowerCountProcessorConfig config = new FollowerCountProcessorConfig();
        log.info("=== Starting FollowerCountProcessorService ===");
        log.info("Kafka Broker: {}", config.getKafkaBroker());
        log.info("Kafka Topic: {}", config.getKafkaTopic());
        log.info("Group ID: {}", config.getKafkaGroupId());
        log.info("Cassandra Host: {}, Port: {}, Keyspace: {}",
                config.getCassandraHost(), config.getCassandraPort(), config.getCassandraKeyspace());

        
        Consumer<String, String> consumer = createKafkaConsumer(
                config.getKafkaBroker(),
                config.getKafkaGroupId()
        );
        consumer.subscribe(Collections.singletonList(config.getKafkaTopic()));

        
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(config.getCassandraHost(), config.getCassandraPort()))
                .withLocalDatacenter("datacenter1")
                .withKeyspace(CqlIdentifier.fromCql(config.getCassandraKeyspace()))
                .build()) {

            log.info("Connected to Cassandra successfully.");

            
            PreparedStatement insertTimeline = session.prepare(
                    "INSERT INTO follower_timeline (influencer_id, event_ts, follower_count) "
                            + "VALUES (?, ?, ?)"
            );

            PreparedStatement selectAggregate = session.prepare(
                    "SELECT current_count, total_sum, total_count, avg_count "
                            + "FROM follower_aggregate WHERE influencer_id = ?"
            );

            PreparedStatement insertNewAggregate = session.prepare(
                    "INSERT INTO follower_aggregate (influencer_id, current_count, total_sum, total_count, avg_count) "
                            + "VALUES (?, ?, ?, ?, ?)"
            );

            PreparedStatement updateExistingAggregate = session.prepare(
                    "UPDATE follower_aggregate "
                            + "SET current_count = ?, total_sum = ?, total_count = ?, avg_count = ? "
                            + "WHERE influencer_id = ?"
            );

            
            ObjectMapper mapper = new ObjectMapper();

            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down consumer...");
                consumer.wakeup();
            }));

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        continue;
                    }
                    for (ConsumerRecord<String, String> rec : records) {
                        String jsonValue = rec.value();
                        FollowerData data;
                        try {
                            data = mapper.readValue(jsonValue, FollowerData.class);
                        } catch (JsonProcessingException e) {
                            log.error("Failed to parse JSON: {}", jsonValue, e);
                            continue;
                        }

                        
                        Instant eventTime = Instant.ofEpochMilli(data.getTimestamp());
                        session.executeAsync(insertTimeline.bind(
                                data.getPk(),
                                eventTime,
                                data.getFollowerCount()
                        ));

                        
                        Row existing = session.execute(selectAggregate.bind(data.getPk())).one();
                        if (existing == null) {
                            long sum = data.getFollowerCount();
                            long count = 1;
                            double avg = sum;
                            session.executeAsync(insertNewAggregate.bind(
                                    data.getPk(),
                                    data.getFollowerCount(),
                                    sum,
                                    count,
                                    avg
                            ));
                        } else {
                            long oldSum = existing.getLong("total_sum");
                            long oldCount = existing.getLong("total_count");
                            long newSum = oldSum + data.getFollowerCount();
                            long newCount = oldCount + 1;
                            double newAvg = (double) newSum / newCount;

                            session.executeAsync(updateExistingAggregate.bind(
                                    data.getFollowerCount(),
                                    newSum,
                                    newCount,
                                    newAvg,
                                    data.getPk()
                            ));
                        }
                    }
                }
            } catch (WakeupException we) {
                log.info("WakeupException received, shutting down consumer loop...");
            } finally {
                consumer.close();
                log.info("Consumer closed.");
            }
        }
    }

    private static Consumer<String, String> createKafkaConsumer(String kafkaBroker, String groupId) {
        var props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }
}
