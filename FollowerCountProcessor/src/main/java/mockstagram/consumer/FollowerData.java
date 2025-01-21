package mockstagram.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FollowerData {
    public long pk;
    public String username;
    public int followerCount;
    public long timestamp;

    // Default constructor needed for Jackson
    public FollowerData() { }

    public long getPk() {
        return pk;
    }

    public void setPk(long pk) {
        this.pk = pk;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getFollowerCount() {
        return followerCount;
    }

    public void setFollowerCount(int followerCount) {
        this.followerCount = followerCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "FollowerData{" +
                "pk=" + pk +
                ", username='" + username + '\'' +
                ", followerCount=" + followerCount +
                ", timestamp=" + timestamp +
                '}';
    }

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (IOException e) {
            return "{}";
        }
    }
}