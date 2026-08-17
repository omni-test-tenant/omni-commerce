export class CheckoutService {
  constructor(stores = {}) {
    this.stores = stores;
  }

  async processCheckout({ userId, sku, quantity, amountCents }) {
    const orderId = `ord-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;

    if (this.stores.redis) {
      const session = await this.stores.redis.hgetall(`session:${userId}`);
      if (!session || Object.keys(session).length === 0) {
        throw new Error("Invalid session token");
      }
    }

    if (this.stores.mongodb) {
      const product = await this.stores.mongodb.collection("product_catalogs").findOne({ sku });
      if (!product || product.inventoryStatus !== "in_stock") {
        throw new Error(`Product ${sku} is out of stock`);
      }
    }

    if (this.stores.postgres) {
      await this.stores.postgres.query(
        "INSERT INTO orders (id, user_id, order_number, status, total_amount_cents) VALUES ($1, $2, $3, $4, $5)",
        [orderId, userId, `ORD-${orderId}`, "paid", amountCents]
      );
    }

    if (this.stores.kafka) {
      await this.stores.kafka.send({
        topic: "omnicommerce.order-events",
        messages: [{ key: userId, value: JSON.stringify({ eventType: "OrderCreated", orderId, sku, quantity, amountCents }) }]
      });
    }

    return {
      success: true,
      orderId,
      orderNumber: `ORD-${orderId}`,
      status: "paid",
      totalAmountCents: amountCents
    };
  }
}
