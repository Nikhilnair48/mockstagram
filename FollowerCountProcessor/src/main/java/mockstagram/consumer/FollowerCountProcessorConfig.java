package mockstagram.consumer;

public class FollowerCountProcessorConfig {

    private final String kafkaBroker;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final String cassandraHost;
    private final int cassandraPort;
    private final String cassandraKeyspace;

    public FollowerCountProcessorConfig() {
        this.kafkaBroker = envOrDefault("KAFKA_BROKER", "localhost:9092");
        this.kafkaTopic = envOrDefault("KAFKA_TOPIC", "my_follower_counts");
        this.kafkaGroupId = envOrDefault("KAFKA_GROUP_ID", "follower-count-processor");
        this.cassandraHost = envOrDefault("CASSANDRA_HOST", "127.0.0.1");
        this.cassandraPort = intEnvOrDefault("CASSANDRA_PORT", 9042);
        this.cassandraKeyspace = envOrDefault("CASSANDRA_KEYSPACE", "influencer_ks");
    }

    private static String envOrDefault(String name, String defaultVal) {
        String val = System.getenv(name);
        return (val != null && !val.isEmpty()) ? val : defaultVal;
    }

    private static int intEnvOrDefault(String name, int defaultVal) {
        try {
            return Integer.parseInt(envOrDefault(name, String.valueOf(defaultVal)));
        } catch (NumberFormatException ex) {
            return defaultVal;
        }
    }

    public String getKafkaBroker() { return kafkaBroker; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getKafkaGroupId() { return kafkaGroupId; }
    public String getCassandraHost() { return cassandraHost; }
    public int getCassandraPort() { return cassandraPort; }
    public String getCassandraKeyspace() { return cassandraKeyspace; }
}
