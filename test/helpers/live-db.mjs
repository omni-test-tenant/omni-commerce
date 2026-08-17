import { Client as PgClient } from "pg";
import { MongoClient } from "mongodb";
import Redis from "ioredis";
import { Kafka } from "kafkajs";

export async function createLiveTestStores() {
  const pgUrl = process.env.CDW_TEST_POSTGRES_URL || "postgresql://postgres:cdw-ci-disposable-only@127.0.0.1:5432/postgres";
  const mongoUrl = process.env.CDW_TEST_MONGO_URL || "mongodb://127.0.0.1:27017/omnicommerce_test";
  const redisUrl = process.env.CDW_TEST_REDIS_URL || "redis://127.0.0.1:6379";
  const kafkaBrokers = (process.env.CDW_TEST_KAFKA_BROKERS || "127.0.0.1:9092").split(",");

  const stores = {};
  const cleanups = [];

  // 1. Live PostgreSQL Client
  try {
    const pg = new PgClient({ connectionString: pgUrl, connectionTimeoutMillis: 1500 });
    await pg.connect();
    await pg.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        order_number TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL,
        total_amount_cents INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS order_items (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        sku TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price_cents INTEGER NOT NULL
      );
    `);
    stores.postgres = pg;
    cleanups.push(async () => {
      await pg.query("DROP TABLE IF EXISTS order_items, orders CASCADE").catch(() => {});
      await pg.end().catch(() => {});
    });
  } catch (err) {
    // Falls back only if no live PG available
  }

  // 2. Live MongoDB Client
  try {
    const mongo = new MongoClient(mongoUrl, { serverSelectionTimeoutMS: 1500 });
    await mongo.connect();
    const db = mongo.db();
    stores.mongodb = db;
    cleanups.push(async () => {
      await db.collection("product_catalogs").drop().catch(() => {});
      await mongo.close().catch(() => {});
    });
  } catch (err) {}

  // 3. Live Redis Client
  try {
    const redis = new Redis(redisUrl, { connectTimeout: 1500, maxRetriesPerRequest: 1, retryStrategy: () => null });
    redis.on("error", () => {});
    await redis.ping();
    stores.redis = redis;
    cleanups.push(async () => {
      await redis.quit().catch(() => redis.disconnect());
    });
  } catch (err) {}

  // 4. Live Kafka Client
  try {
    const kafka = new Kafka({ clientId: `omni-test-${Date.now()}`, brokers: kafkaBrokers, retry: { retries: 5 } });
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic: "omnicommerce.order-events", numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: true
    }).catch(() => {});
    await admin.disconnect();

    const producer = kafka.producer({ retry: { retries: 5 } });
    await producer.connect();
    stores.kafka = producer;
    cleanups.push(async () => {
      await producer.disconnect().catch(() => {});
    });
  } catch (err) {}

  return {
    stores,
    cleanup: async () => {
      for (const fn of cleanups.reverse()) {
        await fn().catch(() => {});
      }
    }
  };
}
