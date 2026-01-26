# Supported vs Unsupported (D-Core)

This page tracks what D-Core currently supports, what is planned, and what is explicitly out of scope.

## Supported

### Messaging

- Routing facade (Integrant key + helpers)
- Codecs: EDN, JSON
- Producers: in-memory, Redis, Kafka, JetStream/NATS, RabbitMQ
- Consumers/runtimes: in-memory, Redis Streams, Kafka, JetStream/NATS, RabbitMQ, SQS
- Dead-letter sinks: logger, storage, producer
- Deferred delivery (Quartz-backed scheduler component)

### Cache

- In-memory
- Local file (filesystem-backed)
- Redis-backed
- Valkey-backed
- Memcached-backed
- Common cache facade (select default via config)

### Storage

- Local disk
- MinIO / S3-style backend
- Common storage facade (select default via config)

### Clients

- Redis
- Valkey
- SQS
- Kafka
- JetStream/NATS
- RabbitMQ
- Memcached
- SQLite
- Postgres
- Datomic
- Typesense
- HTTP (policy wrapper: rate-limit, bulkhead, circuit breaker, retries)

### Tracing

- Context helpers (`d-core.tracing`)
- Ring middleware (`:d-core.core.tracing.http/middleware`)

### Authentication and Authorization

- JWT/OIDC authenticator (`:d-core.core.authn.jwt/authenticator`)
- Scope-based authorizer (`:d-core.core.authz.scope/authorizer`)
- Ring middleware (`:d-core.core.auth.http/*`)
- Token client helpers (`:d-core.core.auth/token-client`)

### GraphQL

- Dedicated GraphQL server (Lacinia)
- GraphiQL endpoint (optional)
- Subscriptions via graphql-transport-ws (optional)

### Scheduling

- Cron tasks (Quartz-backed scheduler component)

### Dev/test helpers

- In-memory queues (`:d-core.queue/*`)

## Unsupported (by design)

- Business/domain logic
- HTTP routes/controllers (app-owned)
- App-specific handlers/workers (app-owned)
- Opinionated schemas/validation for your domain payloads

## Planned / TBD

- Split artifacts per backend (keep as single lib for now; consider later)
- More formal compatibility guarantees and versioning policy
