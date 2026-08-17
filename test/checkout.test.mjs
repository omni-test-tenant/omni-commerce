import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "../src/index.mjs";
import { CheckoutService } from "../src/services/checkout.mjs";
import { OutOfStockError, InvalidSessionError, ConcurrencyLockError, IdempotencyConflictError } from "../src/errors.mjs";
import { createLiveTestStores } from "./helpers/live-db.mjs";

test("Contract & Error Hierarchy: typed errors expose deterministic HTTP status codes and properties", () => {
  const stockErr = new OutOfStockError("SKU-123", 2);
  assert.equal(stockErr.statusCode, 409);
  assert.equal(stockErr.name, "OutOfStockError");
  assert.equal(stockErr.sku, "SKU-123");

  const sessionErr = new InvalidSessionError("user-456");
  assert.equal(sessionErr.statusCode, 401);
  assert.equal(sessionErr.name, "InvalidSessionError");
  assert.equal(sessionErr.userId, "user-456");

  const lockErr = new ConcurrencyLockError("cart:user-456");
  assert.equal(lockErr.statusCode, 423);
  assert.equal(lockErr.name, "ConcurrencyLockError");

  const idempErr = new IdempotencyConflictError("key-789");
  assert.equal(idempErr.statusCode, 409);
  assert.equal(idempErr.name, "IdempotencyConflictError");
});

test("Fastify Error Handler maps typed errors to exact HTTP status codes", async () => {
  const app = buildServer({ logger: false });

  app.get("/test-stock-error", async () => {
    throw new OutOfStockError("SKU-SOLD-OUT", 1);
  });
  app.get("/test-session-error", async () => {
    throw new InvalidSessionError("u-unauth");
  });
  app.get("/test-lock-error", async () => {
    throw new ConcurrencyLockError("cart:u-busy");
  });
  app.get("/test-idemp-error", async () => {
    throw new IdempotencyConflictError("key-busy");
  });

  const resStock = await app.inject({ method: "GET", url: "/test-stock-error" });
  assert.equal(resStock.statusCode, 409);
  assert.equal(resStock.json().error, "OutOfStockError");

  const resSession = await app.inject({ method: "GET", url: "/test-session-error" });
  assert.equal(resSession.statusCode, 401);
  assert.equal(resSession.json().error, "InvalidSessionError");

  const resLock = await app.inject({ method: "GET", url: "/test-lock-error" });
  assert.equal(resLock.statusCode, 423);
  assert.equal(resLock.json().error, "ConcurrencyLockError");

  const resIdemp = await app.inject({ method: "GET", url: "/test-idemp-error" });
  assert.equal(resIdemp.statusCode, 409);
  assert.equal(resIdemp.json().error, "IdempotencyConflictError");
});

test("Live Zero-Mock: processCheckout executes against real PostgreSQL, MongoDB, Redis, and Kafka", async () => {
  const { stores, cleanup } = await createLiveTestStores();

  try {
    assert.ok(stores.postgres, "Real PostgreSQL connected");
    assert.ok(stores.mongodb, "Real MongoDB connected");
    assert.ok(stores.redis, "Real Redis connected");
    assert.ok(stores.kafka, "Real Kafka connected");

    const userId = "u1000000-0000-4000-8000-000000000001";
    await stores.redis.hset(`session:${userId}`, "authenticated", "true", "role", "customer");

    await stores.mongodb.collection("product_catalogs").insertOne({
      sku: "SKU-OMNI-4K-TV",
      stockQuantity: 5,
      priceCents: 14999
    });

    const checkout = new CheckoutService(stores);

    // 1. Successful checkout across all 4 live engines
    const result = await checkout.processCheckout({
      userId,
      sku: "SKU-OMNI-4K-TV",
      quantity: 2,
      amountCents: 29998
    });

    assert.equal(result.success, true);
    assert.equal(result.status, "paid");

    // Stock in real MongoDB decremented from 5 to 3
    const mongoDoc = await stores.mongodb.collection("product_catalogs").findOne({ sku: "SKU-OMNI-4K-TV" });
    assert.equal(mongoDoc.stockQuantity, 3);

    // Order created in real PostgreSQL
    const pgRes = await stores.postgres.query("SELECT * FROM orders WHERE id = $1", [result.orderId]);
    assert.equal(pgRes.rowCount, 1);
    assert.equal(pgRes.rows[0].status, "paid");

    // 2. Reject out of stock (attempting 4 when only 3 remaining)
    await assert.rejects(
      () => checkout.processCheckout({
        userId,
        sku: "SKU-OMNI-4K-TV",
        quantity: 4,
        amountCents: 59996
      }),
      OutOfStockError
    );

    // Ensure stock remained 3
    const mongoDocAfterFail = await stores.mongodb.collection("product_catalogs").findOne({ sku: "SKU-OMNI-4K-TV" });
    assert.equal(mongoDocAfterFail.stockQuantity, 3);

    // 3. Reject unauthenticated session in real Redis
    await assert.rejects(
      () => checkout.processCheckout({
        userId: "u-non-existent-user",
        sku: "SKU-OMNI-4K-TV",
        quantity: 1,
        amountCents: 14999
      }),
      InvalidSessionError
    );
  } finally {
    await cleanup();
  }
});

test("Live Zero-Mock: Distributed Compensation on Kafka Failure across real PostgreSQL and MongoDB", async () => {
  const { stores, cleanup } = await createLiveTestStores();

  try {
    assert.ok(stores.postgres, "Real PostgreSQL connected");
    assert.ok(stores.mongodb, "Real MongoDB connected");
    assert.ok(stores.redis, "Real Redis connected");

    const userId = "u-kafka-fail-user";
    await stores.redis.hset(`session:${userId}`, "authenticated", "true", "role", "customer");

    await stores.mongodb.collection("product_catalogs").insertOne({
      sku: "SKU-KAFKA-FAIL",
      stockQuantity: 10,
      priceCents: 5000
    });

    // Create a real Kafka client pointing to a bad topic to force Kafka send rejection
    const failingKafka = {
      send: async () => {
        throw new Error("Simulated broker network partition");
      }
    };

    const checkout = new CheckoutService({
      ...stores,
      kafka: failingKafka
    });

    await assert.rejects(
      () => checkout.processCheckout({
        userId,
        sku: "SKU-KAFKA-FAIL",
        quantity: 3,
        amountCents: 15000
      }),
      /Simulated broker network partition/
    );

    // 1. Verify MongoDB inventory was compensated back to 10 (not decremented to 7)
    const doc = await stores.mongodb.collection("product_catalogs").findOne({ sku: "SKU-KAFKA-FAIL" });
    assert.equal(doc.stockQuantity, 10);

    // 2. Verify PostgreSQL order was compensated to 'failed' status
    const pgRes = await stores.postgres.query("SELECT * FROM orders WHERE user_id = $1", [userId]);
    assert.equal(pgRes.rowCount, 1);
    assert.equal(pgRes.rows[0].status, "failed");
  } finally {
    await cleanup();
  }
});

test("Live Zero-Mock: Fastify Server with Idempotency Middleware on real Redis", async () => {
  const { stores, cleanup } = await createLiveTestStores();

  try {
    const app = buildServer({ stores, logger: false });
    const userId = "u-idemp-user-1";
    await stores.redis.hset(`session:${userId}`, "authenticated", "true", "role", "customer");

    await stores.mongodb.collection("product_catalogs").insertOne({
      sku: "SKU-IDEMP-TEST",
      stockQuantity: 10,
      priceCents: 5000
    });

    const idempotencyKey = `idemp-live-key-${Date.now()}`;

    // Request 1
    const res1 = await app.inject({
      method: "POST",
      url: "/api/v1/checkout",
      headers: { "x-idempotency-key": idempotencyKey },
      payload: { userId, sku: "SKU-IDEMP-TEST", quantity: 1, amountCents: 5000 }
    });
    assert.equal(res1.statusCode, 201);
    const data1 = res1.json();

    // Request 2 with same idempotency key (must return exact cached response from real Redis)
    const res2 = await app.inject({
      method: "POST",
      url: "/api/v1/checkout",
      headers: { "x-idempotency-key": idempotencyKey },
      payload: { userId, sku: "SKU-IDEMP-TEST", quantity: 1, amountCents: 5000 }
    });
    assert.equal(res2.statusCode, 201);
    const data2 = res2.json();

    assert.equal(data2.orderId, data1.orderId);
    assert.equal(data2.orderNumber, data1.orderNumber);

    // Verify stock in real MongoDB only decremented by 1 (from 10 to 9)
    const doc = await stores.mongodb.collection("product_catalogs").findOne({ sku: "SKU-IDEMP-TEST" });
    assert.equal(doc.stockQuantity, 9);
  } finally {
    await cleanup();
  }
});
