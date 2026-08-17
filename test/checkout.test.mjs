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

test("Live Zero-Mock Multi-Store Checkout: executes against real database drivers when available", async (t) => {
  const { stores, cleanup } = await createLiveTestStores();

  try {
    if (!stores.postgres || !stores.mongodb || !stores.redis) {
      t.skip("Live database services not reachable on local ports - skipping live store run");
      return;
    }

    // Seed session in real Redis
    const userId = "u1000000-0000-4000-8000-000000000001";
    await stores.redis.hset(`session:${userId}`, "authenticated", "true", "role", "customer");

    // Seed inventory in real MongoDB
    await stores.mongodb.collection("product_catalogs").insertOne({
      sku: "SKU-OMNI-4K-TV",
      stockQuantity: 5,
      priceCents: 14999
    });

    const checkout = new CheckoutService(stores);
    const result = await checkout.processCheckout({
      userId,
      sku: "SKU-OMNI-4K-TV",
      quantity: 1,
      amountCents: 14999
    });

    assert.equal(result.success, true);
    assert.equal(result.status, "paid");
    assert.equal(result.totalAmountCents, 14999);

    // Verify stock decreased to 4 in real MongoDB
    const updatedMongoDoc = await stores.mongodb.collection("product_catalogs").findOne({ sku: "SKU-OMNI-4K-TV" });
    assert.equal(updatedMongoDoc.stockQuantity, 4);

    // Verify row created in real PostgreSQL
    const pgRes = await stores.postgres.query("SELECT * FROM orders WHERE id = $1", [result.orderId]);
    assert.equal(pgRes.rowCount, 1);
    assert.equal(pgRes.rows[0].status, "paid");
  } finally {
    await cleanup();
  }
});
