package mockstagram.retriever;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MockstagramApiClient {
    private static final Logger log = LoggerFactory.getLogger(MockstagramApiClient.class);

    private final String baseUrl;
    private final ObjectMapper objectMapper;

    public MockstagramApiClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.objectMapper = new ObjectMapper();
    }

    public FollowerData fetchFollowerCount(Long influencerId) {
        String url = baseUrl + influencerId;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    FollowerData data = objectMapper.readValue(
                            response.getEntity().getContent(), FollowerData.class
                    );
                    data.timestamp = System.currentTimeMillis();
                    return data;
                } else {
                    log.warn("Non-2xx status code: {} for influencerId {}", statusCode, influencerId);
                }
            }
        } catch (IOException e) {
            log.error("Error calling Mockstagram API for influencerId {}", influencerId, e);
        }
        return null;
    }
}
