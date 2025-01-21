// Usage: node seed_redis.js <startId> <endId> [redisKey]
// Example: 
// node seed_redis.js 1000001 1000003 influencer_ids
// node seed_redis.js 1000001 1999999 influencer_ids

const redis = require("redis");

async function main() {
  // 1. Validate command-line args
  if (process.argv.length < 4) {
    console.error("Usage: node seed_redis.js <startId> <endId> [redisKey]");
    process.exit(1);
  }

  const startId = parseInt(process.argv[2], 10);
  const endId = parseInt(process.argv[3], 10);
  const redisKey = process.argv[4] || "influencer_ids";

  if (isNaN(startId) || isNaN(endId)) {
    console.error("startId and endId must be valid integers.");
    process.exit(1);
  }

  // 2. Connect to Redis (default: localhost:6379)
  //    For a remote host or different port, set REDIS_URL=redis://host:port
  //    or pass redis.createClient({ url: 'redis://...' })
  const client = redis.createClient();

  client.on("error", (err) => {
    console.error("Redis Client Error", err);
    process.exit(1);
  });

  await client.connect();

  // 3. Seed the Redis set
  console.log(`Seeding IDs from ${startId} to ${endId} into set "${redisKey}"...`);
  const pipeline = client.multi(); // pipeline for efficiency

  for (let id = startId; id <= endId; id++) {
    pipeline.sAdd(redisKey, id.toString());
  }

  await pipeline.exec();

  // 4. Confirm how many we inserted
  const size = await client.sCard(redisKey);
  console.log(`Done. The Redis set "${redisKey}" now has ${size} total items.`);

  // 5. Disconnect
  await client.quit();
}

main().catch(err => {
  console.error("Unexpected error:", err);
  process.exit(1);
});
