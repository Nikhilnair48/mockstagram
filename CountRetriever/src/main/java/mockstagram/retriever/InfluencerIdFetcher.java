package mockstagram.retriever;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class InfluencerIdFetcher {
    private static final Logger log = LoggerFactory.getLogger(InfluencerIdFetcher.class);

    private final String redisHost;
    private final int redisPort;
    private final String influencerKey;

    public InfluencerIdFetcher(String redisHost, int redisPort, String influencerKey) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.influencerKey = influencerKey;
    }

    /**
     * Loads all influencer IDs from a Redis
     */
    public List<Long> loadInfluencerIds() {
        List<Long> result = new ArrayList<>();
        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            Set<String> members = jedis.smembers(influencerKey);
            for (String m : members) {
                try {
                    result.add(Long.parseLong(m));
                } catch (NumberFormatException e) {
                    log.warn("Invalid influencer ID in Redis: {}", m);
                }
            }
            log.info("Loaded {} influencer IDs from Redis (key: {})", result.size(), influencerKey);
        } catch (Exception e) {
            log.error("Failed to load influencer IDs from Redis", e);
        }
        return result;
    }
}
