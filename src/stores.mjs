import { Client as PgClient } from "pg";
import { MongoClient } from "mongodb";
import Redis from "ioredis";
import { Kafka } from "kafkajs";

/**
 * Build the live store clients the checkout path writes through.
 *
 * `src/index.mjs` used to call `buildServer()` with no stores at all. Every store access
 * in CheckoutService is guarded by `if (this.stores.X)`, so the running service accepted
 * checkouts and answered `201 {status: "paid"}` having written to nothing — no order row,
 * no inventory decrement, no event, and no idempotency middleware (which only registers
 * when a Redis client is present). It was a stub that always said yes, and nothing in the
 * suites noticed because they construct their own stores and never exercise the entrypoint.
 *
 * Two rules follow from that, and they are the point of this module:
 *
 * A store that is configured but unreachable is a startup failure, not a downgrade. The
 * test helper deliberately records connection errors and carries on, because a test may
 * legitimately run against a subset of engines. A service doing the same thing silently
 * resumes the stub behaviour for whichever engine happens to be down.
 *
 * Persistence is not optional. `postgres` is required, so no configuration mistake can
 * produce a server that reports paid orders it did not store.
 */

const REQUIRED = "postgres";

export async function createStoresFromEnv(env = process.env) {
  const postgresUrl = env.OMNI_POSTGRES_URL;
  const mongoUrl = env.OMNI_MONGO_URL;
  const redisUrl = env.OMNI_REDIS_URL;
  const kafkaBrokers = env.OMNI_KAFKA_BROKERS;

  if (!postgresUrl) {
    throw new Error(
      "OMNI_POSTGRES_URL is required. Without it the checkout route would answer 201 for " +
        "orders it never stored."
    );
  }

  const stores = {};
  const closers = [];

  const postgres = new PgClient({ connectionString: postgresUrl, connectionTimeoutMillis: 5000 });
  await connectOrFail("postgres", () => postgres.connect());
  stores.postgres = postgres;
  closers.push(() => postgres.end());

  if (mongoUrl) {
    const mongo = new MongoClient(mongoUrl, { serverSelectionTimeoutMS: 5000 });
    await connectOrFail("mongodb", () => mongo.connect());
    stores.mongodb = mongo.db();
    closers.push(() => mongo.close());
  }

  if (redisUrl) {
    // `retryStrategy: () => null` so an unreachable Redis surfaces here instead of the
    // client reconnecting forever behind a server that has already started serving.
    const redis = new Redis(redisUrl, { connectTimeout: 5000, maxRetriesPerRequest: 1, retryStrategy: () => null });
    redis.on("error", () => {});
    await connectOrFail("redis", () => redis.ping());
    stores.redis = redis;
    closers.push(() => redis.quit());
  }

  if (kafkaBrokers) {
    const kafka = new Kafka({
      clientId: env.OMNI_KAFKA_CLIENT_ID ?? "omni-commerce",
      brokers: kafkaBrokers.split(",").map((b) => b.trim()).filter(Boolean),
      retry: { retries: 5 }
    });
    const producer = kafka.producer({ retry: { retries: 5 } });
    await connectOrFail("kafka", () => producer.connect());
    stores.kafka = producer;
    closers.push(() => producer.disconnect());
  }

  return {
    stores,
    configured: Object.keys(stores),
    close: async () => {
      for (const close of closers.reverse()) {
        await close().catch(() => {});
      }
    }
  };
}

/**
 * Name the engine in the failure. "connect ECONNREFUSED 127.0.0.1:5432" alone does not say
 * which of four engines was being reached, and the ports are allocated per run.
 */
async function connectOrFail(name, connect) {
  try {
    await connect();
  } catch (err) {
    const suffix = name === REQUIRED ? "" : " (configured, so it is required)";
    throw new Error(`${name} is unreachable${suffix}: ${err.message}`);
  }
}
