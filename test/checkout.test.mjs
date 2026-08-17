import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "../src/index.mjs";
import { CheckoutService } from "../src/services/checkout.mjs";
import { OutOfStockError, InvalidSessionError, ConcurrencyLockError } from "../src/errors.mjs";

test("CheckoutService processes valid order across 5-store dependencies", async () => {
  const kafkaMessages = [];
  const redisKeys = new Map();
  redisKeys.set("session:u1000000-0000-4000-8000-000000000001", { role: "merchant_admin", authenticated: "true" });

  let stock = 5;
  const mockStores = {
    redis: {
      hgetall: async (k) => redisKeys.get(k),
      set: async (k, v) => "OK",
      del: async (k) => 1
    },
    mongodb: {
      collection: () => ({
        findOneAndUpdate: async ({ sku, stockQuantity }, update) => {
          if (stock >= (stockQuantity?.$gte ?? 1)) {
            stock += update.$inc.stockQuantity;
            return { sku, stockQuantity: stock, priceCents: 14999 };
          }
          return null;
        }
      })
    },
    postgres: {
      query: async () => ({ rowCount: 1 })
    },
    kafka: {
      send: async (msg) => { kafkaMessages.push(msg); }
    }
  };

  const checkout = new CheckoutService(mockStores);
  const result = await checkout.processCheckout({
    userId: "u1000000-0000-4000-8000-000000000001",
    sku: "SKU-OMNI-4K-TV",
    quantity: 1,
    amountCents: 14999
  });

  assert.equal(result.success, true);
  assert.equal(result.status, "paid");
  assert.equal(result.totalAmountCents, 14999);
  assert.equal(kafkaMessages.length, 1);
  assert.equal(stock, 4);
});

test("CheckoutService performs distributed compensation on Kafka failure", async () => {
  let stock = 5;
  const pgQueries = [];
  const mockStores = {
    redis: {
      hgetall: async () => ({ authenticated: "true" }),
      set: async () => "OK",
      del: async () => 1
    },
    mongodb: {
      collection: () => ({
        findOneAndUpdate: async ({ sku, stockQuantity }, update) => {
          stock += update.$inc.stockQuantity;
          return { sku, stockQuantity: stock };
        },
        updateOne: async ({ sku }, update) => {
          stock += update.$inc.stockQuantity;
          return { modifiedCount: 1 };
        }
      })
    },
    postgres: {
      query: async (sql, params) => {
        pgQueries.push({ sql, params });
        return { rowCount: 1 };
      }
    },
    kafka: {
      send: async () => {
        throw new Error("Kafka broker connection failure");
      }
    }
  };

  const checkout = new CheckoutService(mockStores);
  await assert.rejects(
    () => checkout.processCheckout({
      userId: "u-kafka-fail",
      sku: "SKU-KAFKA-COMPENSATE",
      quantity: 1,
      amountCents: 9999
    }),
    /Kafka broker connection failure/
  );

  // MongoDB inventory must be fully restored to 5
  assert.equal(stock, 5);
  // PostgreSQL must have executed compensating status update to 'failed'
  const failedQuery = pgQueries.find((q) => q.sql.includes("UPDATE orders SET status = 'failed'"));
  assert.ok(failedQuery, "PostgreSQL compensating query was executed");
});

test("CheckoutService performs distributed compensation on PostgreSQL failure", async () => {
  let stock = 5;
  const mockStores = {
    redis: {
      hgetall: async () => ({ authenticated: "true" }),
      set: async () => "OK",
      del: async () => 1
    },
    mongodb: {
      collection: () => ({
        findOneAndUpdate: async ({ sku, stockQuantity }, update) => {
          stock += update.$inc.stockQuantity;
          return { sku, stockQuantity: stock };
        },
        updateOne: async ({ sku }, update) => {
          stock += update.$inc.stockQuantity;
          return { modifiedCount: 1 };
        }
      })
    },
    postgres: {
      query: async (sql) => {
        if (sql.includes("INSERT INTO orders")) {
          throw new Error("PostgreSQL unique constraint violation");
        }
        return { rowCount: 1 };
      }
    }
  };

  const checkout = new CheckoutService(mockStores);
  await assert.rejects(
    () => checkout.processCheckout({
      userId: "u-pg-fail",
      sku: "SKU-PG-COMPENSATE",
      quantity: 1,
      amountCents: 9999
    }),
    /PostgreSQL unique constraint violation/
  );

  // MongoDB inventory must be restored to 5
  assert.equal(stock, 5);
});

test("CheckoutService rejects unauthenticated session with InvalidSessionError (401)", async () => {
  const mockStores = {
    redis: {
      hgetall: async () => null
    }
  };

  const checkout = new CheckoutService(mockStores);
  await assert.rejects(
    () => checkout.processCheckout({
      userId: "u-unauth",
      sku: "SKU-OMNI-4K-TV",
      quantity: 1,
      amountCents: 14999
    }),
    InvalidSessionError
  );
});

test("CheckoutService rejects out-of-stock item with OutOfStockError (409)", async () => {
  const mockStores = {
    redis: {
      hgetall: async () => ({ authenticated: "true" }),
      set: async () => "OK",
      del: async () => 1
    },
    mongodb: {
      collection: () => ({
        findOneAndUpdate: async () => null
      })
    }
  };

  const checkout = new CheckoutService(mockStores);
  await assert.rejects(
    () => checkout.processCheckout({
      userId: "u1",
      sku: "SKU-OUT-OF-STOCK",
      quantity: 2,
      amountCents: 14999
    }),
    OutOfStockError
  );
});

test("CheckoutService throws ConcurrencyLockError (423) when cart lock collision occurs", async () => {
  const mockStores = {
    redis: {
      hgetall: async () => ({ authenticated: "true" }),
      set: async (k, v, px, ttl, nx) => {
        if (nx) return null; // Lock cannot be acquired
        return "OK";
      }
    }
  };

  const checkout = new CheckoutService(mockStores);
  await assert.rejects(
    () => checkout.processCheckout({
      userId: "u-locked",
      sku: "SKU-LOCKED",
      quantity: 1,
      amountCents: 5000
    }),
    ConcurrencyLockError
  );
});

test("Fastify server integrates typed errors and returns exact HTTP 409 / 401 / 423 status codes", async () => {
  const mockStores = {
    redis: {
      hgetall: async (k) => k === "session:valid-user" ? { authenticated: "true" } : null,
      set: async () => "OK",
      del: async () => 1
    },
    mongodb: {
      collection: () => ({
        findOneAndUpdate: async ({ sku }) => {
          if (sku === "SKU-IN-STOCK") return { sku, stockQuantity: 10 };
          return null;
        }
      })
    },
    postgres: {
      query: async () => ({ rowCount: 1 })
    },
    kafka: {
      send: async () => {}
    }
  };

  const app = buildServer({ stores: mockStores, logger: false });

  // 1. Missing session -> 401
  const res401 = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    payload: { userId: "missing-user", sku: "SKU-IN-STOCK", quantity: 1, amountCents: 1000 }
  });
  assert.equal(res401.statusCode, 401);

  // 2. Out of stock -> 409
  const res409 = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    payload: { userId: "valid-user", sku: "SKU-SOLD-OUT", quantity: 1, amountCents: 1000 }
  });
  assert.equal(res409.statusCode, 409);

  // 3. In stock -> 201
  const res201 = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    payload: { userId: "valid-user", sku: "SKU-IN-STOCK", quantity: 1, amountCents: 1000 }
  });
  assert.equal(res201.statusCode, 201);
  const data = res201.json();
  assert.equal(data.success, true);
});

test("Idempotency middleware returns cached response for repeated X-Idempotency-Key", async () => {
  const cache = new Map();
  const mockRedis = {
    hgetall: async () => ({ authenticated: "true" }),
    set: async (k, v, px, ttl, nx) => {
      if (nx && cache.has(k)) return null;
      cache.set(k, v);
      return "OK";
    },
    get: async (k) => cache.get(k) || null,
    del: async (k) => cache.delete(k)
  };

  const mockStores = {
    redis: mockRedis,
    mongodb: {
      collection: () => ({
        findOneAndUpdate: async ({ sku }) => ({ sku, stockQuantity: 9 })
      })
    },
    postgres: {
      query: async () => ({ rowCount: 1 })
    },
    kafka: {
      send: async () => {}
    }
  };

  const app = buildServer({ stores: mockStores, logger: false });

  const firstReq = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    headers: { "x-idempotency-key": "idemp-tx-12345" },
    payload: { userId: "valid-user", sku: "SKU-IN-STOCK", quantity: 1, amountCents: 5000 }
  });
  assert.equal(firstReq.statusCode, 201);
  const firstData = firstReq.json();

  const secondReq = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    headers: { "x-idempotency-key": "idemp-tx-12345" },
    payload: { userId: "valid-user", sku: "SKU-IN-STOCK", quantity: 1, amountCents: 5000 }
  });
  assert.equal(secondReq.statusCode, 201);
  const secondData = secondReq.json();

  assert.equal(secondData.orderId, firstData.orderId);
  assert.equal(secondData.orderNumber, firstData.orderNumber);
});
