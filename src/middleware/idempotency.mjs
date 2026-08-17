import { IdempotencyConflictError } from "../errors.mjs";

/** Parse a stored idempotency record, returning null when it is not usable. */
function parseIdempotencyRecord(raw) {
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

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
        const parsed = parseIdempotencyRecord(existing);
        if (parsed === null) {
          // An unparseable record is not a valid in-flight request. Swallowing the
          // parse error and falling through to a conflict wedges this key until its
          // TTL expires, so the corrupt entry is dropped and the request proceeds.
          fastify.log?.warn?.({ idempotencyKey }, "discarding unparseable idempotency record");
          await redisClient.del(redisKey).catch(() => {});
          throw new IdempotencyConflictError(idempotencyKey);
        }
        if (parsed.state === "COMPLETED") {
          return reply
            .code(parsed.statusCode || 200)
            .headers(parsed.headers || {})
            .send(parsed.body);
        }
      }
      throw new IdempotencyConflictError(idempotencyKey);
    }
  });

  fastify.addHook("onSend", async (request, reply, payload) => {
    const idempotencyKey = request.idempotencyKey;
    if (!idempotencyKey || !redisClient) return payload;

    const redisKey = `idempotency:${idempotencyKey}`;
    if (reply.statusCode >= 200 && reply.statusCode < 300) {
      // A non-JSON payload is cached verbatim; only JSON bodies are structurally
      // replayable, and coercing the rest would corrupt the replayed response.
      const parsedPayload = parseIdempotencyRecord(payload);
      const bodyToCache = parsedPayload === null ? payload : parsedPayload;
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
      // On non-success, release processing key immediately so client can retry
      await redisClient.del(redisKey).catch(() => {});
    }
    return payload;
  });

  fastify.addHook("onError", async (request, reply, error) => {
    const idempotencyKey = request.idempotencyKey;
    if (idempotencyKey && redisClient) {
      await redisClient.del(`idempotency:${idempotencyKey}`).catch(() => {});
    }
  });

  fastify.addHook("onResponse", async (request, reply) => {
    const idempotencyKey = request.idempotencyKey;
    if (idempotencyKey && redisClient && reply.statusCode >= 400) {
      const redisKey = `idempotency:${idempotencyKey}`;
      const existing = await redisClient.get(redisKey).catch(() => null);
      if (existing) {
        const parsed = parseIdempotencyRecord(existing);
        // Release the key for both a PROCESSING record and an unparseable one:
        // either way no completed response is cached under it.
        if (parsed === null || parsed.state === "PROCESSING") {
          await redisClient.del(redisKey).catch(() => {});
        }
      }
    }
  });
}
