import { IdempotencyConflictError } from "../errors.mjs";

export function registerIdempotencyMiddleware(fastify, { redisClient } = {}) {
  fastify.addHook("preHandler", async (request, reply) => {
    const idempotencyKey = request.headers["x-idempotency-key"] || request.headers["idempotency-key"];
    if (!idempotencyKey || !redisClient) return;

    request.idempotencyKey = idempotencyKey;
    const redisKey = `idempotency:${idempotencyKey}`;

    // Try to acquire processing lock with 30-second TTL
    const acquired = await redisClient.set(
      redisKey,
      JSON.stringify({ state: "PROCESSING", at: Date.now() }),
      "PX",
      30000,
      "NX"
    );

    if (!acquired) {
      const existing = await redisClient.get(redisKey);
      if (existing) {
        try {
          const parsed = JSON.parse(existing);
          if (parsed.state === "COMPLETED") {
            return reply
              .code(parsed.statusCode || 200)
              .headers(parsed.headers || {})
              .send(parsed.body);
          }
        } catch {}
      }
      throw new IdempotencyConflictError(idempotencyKey);
    }
  });

  fastify.addHook("onSend", async (request, reply, payload) => {
    const idempotencyKey = request.idempotencyKey;
    if (!idempotencyKey || !redisClient) return payload;

    const redisKey = `idempotency:${idempotencyKey}`;
    if (reply.statusCode >= 200 && reply.statusCode < 300) {
      let bodyToCache = payload;
      try {
        bodyToCache = JSON.parse(payload);
      } catch {}
      // Cache completed response for 24 hours (86,400,000 ms)
      await redisClient.set(
        redisKey,
        JSON.stringify({
          state: "COMPLETED",
          statusCode: reply.statusCode,
          body: bodyToCache,
          cachedAt: new Date().toISOString()
        }),
        "PX",
        86400000
      );
    } else {
      // On non-success, release processing key so client can retry
      await redisClient.del(redisKey);
    }
    return payload;
  });

  fastify.addHook("onError", async (request, reply, error) => {
    const idempotencyKey = request.idempotencyKey;
    if (idempotencyKey && redisClient) {
      await redisClient.del(`idempotency:${idempotencyKey}`).catch(() => {});
    }
  });
}
