import Fastify from "fastify";
import { CheckoutService } from "./services/checkout.mjs";
import { registerIdempotencyMiddleware } from "./middleware/idempotency.mjs";
import { createStoresFromEnv } from "./stores.mjs";

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

/**
 * The server is built inside `start()` rather than at module scope, because it can only be
 * built once the store clients are connected — and a store client cannot be connected
 * synchronously. Building at module scope is what forced the old `buildServer()` call to
 * pass no stores at all.
 */
const start = async () => {
  let stores;
  try {
    stores = await createStoresFromEnv();
  } catch (err) {
    // No logger yet: the server does not exist, and starting one to report that it must not
    // start would be the same mistake in a smaller form.
    process.stderr.write(`omni-commerce refusing to start: ${err.message}\n`);
    process.exit(1);
  }

  const fastify = buildServer({ stores: stores.stores });
  fastify.log.info({ stores: stores.configured }, "store clients connected");

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, async () => {
      await fastify.close().catch(() => {});
      await stores.close();
      process.exit(0);
    });
  }

  try {
    const port = process.env.PORT || 3000;
    await fastify.listen({ port: Number(port), host: "0.0.0.0" });
  } catch (err) {
    fastify.log.error(err);
    await stores.close();
    process.exit(1);
  }
};

const isDirectExecution = process.argv[1] && import.meta.url.endsWith(process.argv[1]);
if (isDirectExecution) {
  start();
}
