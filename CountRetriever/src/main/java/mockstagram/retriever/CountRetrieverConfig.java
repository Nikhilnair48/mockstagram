package mockstagram.retriever;

public class CountRetrieverConfig {

    private final String kafkaBroker;
    private final String kafkaTopic;
    private final String redisHost;
    private final int redisPort;
    private final String redisInfluencerKey;
    private final String mockstagramUrl;
    private final int fetchIntervalSeconds;

    public CountRetrieverConfig() {
        this.kafkaBroker = envOrDefault("KAFKA_BROKER", "localhost:9092");
        this.kafkaTopic = envOrDefault("KAFKA_TOPIC", "my_follower_counts");
        this.redisHost = envOrDefault("REDIS_HOST", "127.0.0.1");
        this.redisPort = intEnvOrDefault("REDIS_PORT", 6379);
        this.redisInfluencerKey = envOrDefault("REDIS_INFLUENCER_KEY", "influencer_ids");
        this.mockstagramUrl = envOrDefault("MOCKSTAGRAM_URL", "http://localhost:3000/api/v1/influencers/");
        this.fetchIntervalSeconds = intEnvOrDefault("FETCH_INTERVAL_SEC", 60);
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
    public String getRedisHost() { return redisHost; }
    public int getRedisPort() { return redisPort; }
    public String getRedisInfluencerKey() { return redisInfluencerKey; }
    public String getMockstagramUrl() { return mockstagramUrl; }
    public int getFetchIntervalSeconds() { return fetchIntervalSeconds; }
}
