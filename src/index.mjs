import Fastify from "fastify";
import { CheckoutService } from "./services/checkout.mjs";

const fastify = Fastify({ logger: true });
const checkout = new CheckoutService();

fastify.post("/api/v1/checkout", async (request, reply) => {
  const { userId, sku, quantity, amountCents } = request.body || {};
  try {
    const result = await checkout.processCheckout({ userId, sku, quantity, amountCents });
    return reply.code(201).send(result);
  } catch (err) {
    return reply.code(400).send({ error: err.message });
  }
});

const start = async () => {
  try {
    const port = process.env.PORT || 3000;
    await fastify.listen({ port: Number(port), host: "0.0.0.0" });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

if (process.env.NODE_ENV !== "test") {
  start();
}

export { fastify };
