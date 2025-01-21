-- create_cassandra_schema.cql

-- 1) Create the keyspace (for local single-node Cassandra, we'll use SimpleStrategy).
--    Adjust replication_factor as needed (for a single node, 1 is fine).
CREATE KEYSPACE IF NOT EXISTS influencer_ks
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
  };

-- 2) Switch to that keyspace
USE influencer_ks;

-- 3) Create the table for storing every recorded follower count over time
CREATE TABLE IF NOT EXISTS follower_timeline (
    influencer_id   BIGINT,
    event_ts        TIMESTAMP,
    follower_count  INT,
    PRIMARY KEY ((influencer_id), event_ts)
)
WITH CLUSTERING ORDER BY (event_ts DESC);

-- 4) Create the table for storing current count and rolling average
CREATE TABLE IF NOT EXISTS follower_aggregate (
    influencer_id   BIGINT PRIMARY KEY,
    current_count   INT,
    total_sum       BIGINT,
    total_count     BIGINT,
    avg_count       DOUBLE
);
