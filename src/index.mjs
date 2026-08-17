import Fastify from "fastify";
import { CheckoutService } from "./services/checkout.mjs";
import { registerIdempotencyMiddleware } from "./middleware/idempotency.mjs";

export function buildServer(options = {}) {
  const fastify = Fastify({
    logger: options.logger ?? (process.env.NODE_ENV !== "test")
  });

  const stores = options.stores || {};
  const checkout = new CheckoutService(stores);

  // Custom Error Handler mapping typed errors to exact HTTP status codes
  fastify.setErrorHandler((error, request, reply) => {
    const statusCode =
      error.statusCode ||
      (error.name === "OutOfStockError"
        ? 409
        : error.name === "InvalidSessionError"
        ? 401
        : error.name === "ConcurrencyLockError"
        ? 423
        : error.name === "IdempotencyConflictError"
        ? 409
        : 400);

    return reply.code(statusCode).send({
      error: error.name || "Error",
      message: error.message,
      statusCode
    });
  });

  // Register Idempotency Middleware
  if (stores.redis) {
    registerIdempotencyMiddleware(fastify, { redisClient: stores.redis });
  }

  fastify.post("/api/v1/checkout", async (request, reply) => {
    const { userId, sku, quantity, amountCents } = request.body || {};
    const result = await checkout.processCheckout({ userId, sku, quantity, amountCents });
    return reply.code(201).send(result);
  });

  return fastify;
}

const fastify = buildServer();

const start = async () => {
  try {
    const port = process.env.PORT || 3000;
    await fastify.listen({ port: Number(port), host: "0.0.0.0" });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

const isDirectExecution = process.argv[1] && import.meta.url.endsWith(process.argv[1]);
if (isDirectExecution) {
  start();
}

export { fastify };
