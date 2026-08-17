import assert from "node:assert/strict";
import { test } from "node:test";
import { CheckoutService } from "../src/services/checkout.mjs";

test("CheckoutService processes valid order across simulated 5-store dependencies", async () => {
  const kafkaMessages = [];
  const mockStores = {
    redis: {
      hgetall: async (k) => ({ role: "merchant_admin", authenticated: "true" })
    },
    mongodb: {
      collection: () => ({
        findOne: async ({ sku }) => ({ sku, inventoryStatus: "in_stock", priceCents: 14999 })
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
});
