export class OutOfStockError extends Error {
  constructor(sku, requestedQuantity) {
    super(`Product ${sku} is out of stock for requested quantity ${requestedQuantity ?? 1}`);
    this.name = "OutOfStockError";
    this.statusCode = 409;
    this.sku = sku;
  }
}

export class InvalidSessionError extends Error {
  constructor(userId) {
    super(`Invalid or expired session for user ${userId}`);
    this.name = "InvalidSessionError";
    this.statusCode = 401;
    this.userId = userId;
  }
}

export class ConcurrencyLockError extends Error {
  constructor(resource) {
    super(`Resource lock conflict on ${resource}`);
    this.name = "ConcurrencyLockError";
    this.statusCode = 423;
    this.resource = resource;
  }
}

export class IdempotencyConflictError extends Error {
  constructor(key) {
    super(`Idempotent request with key '${key}' is currently processing`);
    this.name = "IdempotencyConflictError";
    this.statusCode = 409;
    this.key = key;
  }
}
