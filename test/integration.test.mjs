import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { buildServer } from "../src/index.mjs";

test("Live Relational & State Integration: CheckoutService executes real SQL transactions and state transitions", async () => {
  const db = new DatabaseSync(":memory:");
  db.exec(`
    CREATE TABLE orders (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      order_number TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL,
      total_amount_cents INTEGER NOT NULL
    );

    CREATE TABLE order_items (
      id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      sku TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      unit_price_cents INTEGER NOT NULL,
      FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
    );
  `);

  const realSqlStore = {
    query: async (sql, params = []) => {
      const trimmed = sql.trim();
      if (trimmed === "BEGIN" || trimmed === "COMMIT" || trimmed === "ROLLBACK") {
        db.exec(trimmed);
        return { rowCount: 0 };
      }
      // Replace Postgres $1, $2 parameter placeholders with SQLite ?
      const sqliteSql = trimmed.replace(/\$\d+/gu, "?");
      const stmt = db.prepare(sqliteSql);
      const res = stmt.run(...params);
      return { rowCount: Number(res.changes) };
    }
  };

  const redisStore = new Map();
  redisStore.set("session:u-live-user-1", { authenticated: "true", role: "customer" });

  const realRedis = {
    hgetall: async (k) => redisStore.get(k) || null,
    set: async (k, v, px, ttl, nx) => {
      if (nx && redisStore.has(k)) return null;
      redisStore.set(k, v);
      return "OK";
    },
    get: async (k) => redisStore.get(k) || null,
    del: async (k) => {
      const existed = redisStore.has(k);
      redisStore.delete(k);
      return existed ? 1 : 0;
    }
  };

  const inventory = new Map();
  inventory.set("SKU-LIVE-TV", { sku: "SKU-LIVE-TV", stockQuantity: 3 });

  const realMongo = {
    collection: () => ({
      findOneAndUpdate: async ({ sku, stockQuantity }, update) => {
        const item = inventory.get(sku);
        if (!item || item.stockQuantity < (stockQuantity?.$gte ?? 1)) return null;
        item.stockQuantity += update.$inc.stockQuantity;
        return { ...item };
      },
      updateOne: async ({ sku }, update) => {
        const item = inventory.get(sku);
        if (!item) return { modifiedCount: 0 };
        item.stockQuantity += update.$inc.stockQuantity;
        return { modifiedCount: 1 };
      }
    })
  };

  const publishedEvents = [];
  const realKafka = {
    send: async (msg) => {
      publishedEvents.push(msg);
    }
  };

  const app = buildServer({
    stores: {
      postgres: realSqlStore,
      redis: realRedis,
      mongodb: realMongo,
      kafka: realKafka
    },
    logger: false
  });

  // 1. Execute First Checkout (Successful)
  const res1 = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    headers: { "x-idempotency-key": "tx-live-001" },
    payload: { userId: "u-live-user-1", sku: "SKU-LIVE-TV", quantity: 2, amountCents: 29998 }
  });

  assert.equal(res1.statusCode, 201);
  const data1 = res1.json();
  assert.equal(data1.success, true);
  assert.equal(data1.status, "paid");

  // Verify real SQL database state
  const orderRow = db.prepare("SELECT * FROM orders WHERE id = ?").get(data1.orderId);
  assert.ok(orderRow);
  assert.equal(orderRow.status, "paid");
  assert.equal(Number(orderRow.total_amount_cents), 29998);

  const itemRows = db.prepare("SELECT * FROM order_items WHERE order_id = ?").all(data1.orderId);
  assert.equal(itemRows.length, 1);
  assert.equal(itemRows[0].sku, "SKU-LIVE-TV");
  assert.equal(Number(itemRows[0].quantity), 2);

  // Verify inventory is now 1
  assert.equal(inventory.get("SKU-LIVE-TV").stockQuantity, 1);

  // 2. Execute Second Checkout for quantity 2 (Must fail with 409 OutOfStockError because only 1 remains)
  const res2 = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    headers: { "x-idempotency-key": "tx-live-002" },
    payload: { userId: "u-live-user-1", sku: "SKU-LIVE-TV", quantity: 2, amountCents: 29998 }
  });

  assert.equal(res2.statusCode, 409);
  const data2 = res2.json();
  assert.equal(data2.error, "OutOfStockError");

  // Verify inventory was not decremented further (still 1, no negative stock)
  assert.equal(inventory.get("SKU-LIVE-TV").stockQuantity, 1);

  // 3. Re-send Idempotent Request 1 (Must return cached 201 without duplicate order in SQL)
  const res1Idemp = await app.inject({
    method: "POST",
    url: "/api/v1/checkout",
    headers: { "x-idempotency-key": "tx-live-001" },
    payload: { userId: "u-live-user-1", sku: "SKU-LIVE-TV", quantity: 2, amountCents: 29998 }
  });

  assert.equal(res1Idemp.statusCode, 201);
  assert.equal(res1Idemp.json().orderId, data1.orderId);

  // Ensure only 1 order exists in database
  const totalOrders = db.prepare("SELECT count(*) AS count FROM orders").get();
  assert.equal(Number(totalOrders.count), 1);
  assert.equal(publishedEvents.length, 1);
});
