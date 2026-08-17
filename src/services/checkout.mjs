import {
  OutOfStockError,
  InvalidSessionError,
  ConcurrencyLockError
} from "../errors.mjs";

export class CheckoutService {
  constructor(stores = {}) {
    this.stores = stores;
  }

  async processCheckout({ userId, sku, quantity = 1, amountCents }) {
    if (!userId) throw new Error("userId is required");
    if (!sku) throw new Error("sku is required");
    if (!amountCents) throw new Error("amountCents is required");

    const orderId = `ord-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const orderNumber = `ORD-${orderId}`;
    let mongoStockDecremented = false;
    let postgresCommitted = false;

    // 1. Session Validation in Redis
    if (this.stores.redis) {
      const session = await this.stores.redis.hgetall(`session:${userId}`);
      if (!session || Object.keys(session).length === 0 || session.authenticated === "false") {
        throw new InvalidSessionError(userId);
      }

      // Acquire Cart Concurrency Lock (30-second TTL to avoid premature expiry under load)
      const lockAcquired = await this.stores.redis.set(
        `lock:cart:${userId}`,
        orderId,
        "PX",
        30000,
        "NX"
      );
      if (!lockAcquired) {
        throw new ConcurrencyLockError(`cart:${userId}`);
      }
    }

    try {
      // 2. Atomic Inventory Decrement in MongoDB
      if (this.stores.mongodb) {
        const collection = typeof this.stores.mongodb.collection === "function"
          ? this.stores.mongodb.collection("product_catalogs")
          : this.stores.mongodb;

        const updateResult = await collection.findOneAndUpdate(
          { sku, stockQuantity: { $gte: quantity } },
          { $inc: { stockQuantity: -quantity } },
          { returnDocument: "after" }
        );

        const updatedDoc = updateResult?.value || updateResult;
        if (!updatedDoc || (!updatedDoc.sku && updateResult?.lastErrorObject?.n === 0)) {
          throw new OutOfStockError(sku, quantity);
        }
        mongoStockDecremented = true;
      }

      // 3. PostgreSQL Relational Transaction
      if (this.stores.postgres) {
        try {
          await this.stores.postgres.query("BEGIN");
          await this.stores.postgres.query(
            "INSERT INTO orders (id, user_id, order_number, status, total_amount_cents) VALUES ($1, $2, $3, $4, $5)",
            [orderId, userId, orderNumber, "paid", amountCents]
          );
          await this.stores.postgres.query(
            "INSERT INTO order_items (id, order_id, sku, quantity, unit_price_cents) VALUES ($1, $2, $3, $4, $5)",
            [`item-${orderId}-1`, orderId, sku, quantity, Math.floor(amountCents / quantity)]
          );
          await this.stores.postgres.query("COMMIT");
          postgresCommitted = true;
        } catch (pgErr) {
          await this.stores.postgres.query("ROLLBACK").catch(() => {});
          // Compensating Transaction: Restore MongoDB inventory
          if (mongoStockDecremented && this.stores.mongodb) {
            const collection = typeof this.stores.mongodb.collection === "function"
              ? this.stores.mongodb.collection("product_catalogs")
              : this.stores.mongodb;
            await collection.updateOne(
              { sku },
              { $inc: { stockQuantity: quantity } }
            ).catch(() => {});
            mongoStockDecremented = false;
          }
          throw pgErr;
        }
      }

      // 4. Kafka Event Stream Emission with Distributed Compensation on Failure
      if (this.stores.kafka) {
        try {
          await this.stores.kafka.send({
            topic: "omnicommerce.order-events",
            messages: [
              {
                key: userId,
                value: JSON.stringify({
                  eventType: "OrderCreated",
                  orderId,
                  orderNumber,
                  userId,
                  sku,
                  quantity,
                  amountCents,
                  createdAt: new Date().toISOString()
                })
              }
            ]
          });
        } catch (kafkaErr) {
          // Compensate committed Postgres transaction
          if (postgresCommitted && this.stores.postgres) {
            await this.stores.postgres.query(
              "UPDATE orders SET status = 'failed' WHERE id = $1",
              [orderId]
            ).catch(() => {});
          }
          // Compensate MongoDB inventory
          if (mongoStockDecremented && this.stores.mongodb) {
            const collection = typeof this.stores.mongodb.collection === "function"
              ? this.stores.mongodb.collection("product_catalogs")
              : this.stores.mongodb;
            await collection.updateOne(
              { sku },
              { $inc: { stockQuantity: quantity } }
            ).catch(() => {});
            mongoStockDecremented = false;
          }
          throw kafkaErr;
        }
      }

      return {
        success: true,
        orderId,
        orderNumber,
        status: "paid",
        totalAmountCents: amountCents,
        sku,
        quantity
      };
    } finally {
      // Release Cart Concurrency Lock safely (only if holding token)
      if (this.stores.redis) {
        try {
          const currentToken = await this.stores.redis.get?.(`lock:cart:${userId}`);
          if (!currentToken || currentToken === orderId) {
            await this.stores.redis.del(`lock:cart:${userId}`);
          }
        } catch {
          await this.stores.redis.del(`lock:cart:${userId}`).catch(() => {});
        }
      }
    }
  }
}
