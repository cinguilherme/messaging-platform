# D-Core Consumer LLM Guide (Single File)

This file is meant to live in any Duct repo that depends on D-Core.
It is self-contained and does not assume external docs or network access.

## Core idea

- D-Core is infrastructure: Integrant components, protocols, and defaults.
- Your app owns business logic, handlers, and routing decisions.
- Wire behavior in `duct.edn` (or your config entrypoint) using `#ig/ref`.

## Rules of engagement for agents

- Do not put business logic inside D-Core components.
- Prefer swapping backends via config, not new code paths.
- Protocols take primitives and accept `opts` as the last argument.
- Keep handlers pure: input is data, output is data.

## Capability map (diagram -> D-Core)

- HTTP ingress -> Ring routes + `:d-core.core.tracing.http/middleware`
- GraphQL API -> `:d-core.module/graphql`
- Async flows -> `:d-core.core.messaging/routing` + producers + consumers
- Dead letters -> `:d-core.core.messaging.dead-letter/*`
- Cache -> `:d-core.core.cache.common/common`
- Object storage -> `:d-core.core.storage/common`
- SQL DB -> `:d-core.core.databases.sql/common`
- Datomic -> `:d-core.core.databases.datomic/db`
- HTTP clients -> `:d-core.core.http/client` or `:d-core.core.http/clients`
- Search -> `:d-core.core.text-search/common`
- Geo -> `:d-core.core.geo.tile38/index`
- Rate limiting -> `:d-core.core.rate-limit.*`
- Cron jobs -> `:d-core.libs.cron-task/scheduler`
- Metrics -> `:d-core.core.metrics.prometheus/*`
- Dev/test queues -> `:d-core.queue/*`

## Messaging model (must-know)

Routing map shape:

- `:topics` define metadata (schema, naming, deadletter defaults).
- `:publish` defines explicit publish targets (fanout).
- `:subscriptions` is the only source of consumers.

Producer behavior:

- `produce!` takes a `msg-map` and `options`.
- `:topic` in options chooses the routing entry.
- One produce call can fan out to multiple targets.

Consumer behavior:

- A subscription runs only if listed under `:subscriptions`.
- Each subscription references a handler function from the app.
- Handlers receive a decoded envelope, usually:
  `{:msg <payload> :options <produce-options> :metadata {...} :produced-at <ms>}`

Multi-cluster behavior:

- Use multiple client components and multiple producer components.
- Producers are mapped by key inside `:d-core.core.producers.common/producer`.
- Consumers select the client with `:client` in each subscription.

### Minimal messaging wiring (Redis example)

```edn
{:system
 ;; Routing map owned by the app
 :my-app.config.messaging/routing
 {:topics {:orders {}}
  :publish {:orders {:targets [{:producer :redis-primary
                                :stream "core:orders"}]}}
  :subscriptions {:orders-worker {:topic :orders
                                  :source :redis
                                  :client :redis-primary
                                  :handler #ig/ref :my-app.handlers/orders}}}

 :d-core.core.messaging/routing #ig/ref :my-app.config.messaging/routing

 :d-core.core.messaging.codecs/json {}
 :d-core.core.messaging/codec {:codec #ig/ref :d-core.core.messaging.codecs/json}

 :d-core.core.clients.redis/client {:uri "redis://localhost:6379"}

 :d-core.core.producers.redis/producer
 {:redis #ig/ref :d-core.core.clients.redis/client
  :routing #ig/ref :d-core.core.messaging/routing
  :codec #ig/ref :d-core.core.messaging/codec}

 :d-core.core.producers.common/producer
 {:default-producer :redis-primary
  :routing #ig/ref :d-core.core.messaging/routing
  :producers {:redis-primary #ig/ref :d-core.core.producers.redis/producer}}

 :d-core.core.consumers.redis/runtime
 {:redis {:redis-primary #ig/ref :d-core.core.clients.redis/client}
  :routing #ig/ref :d-core.core.messaging/routing
  :codec #ig/ref :d-core.core.messaging/codec}}}
```

## Dead letters (DLQ) behavior

DLQ config merges in this order:

1. `[:defaults :deadletter]`
2. `[:topics <topic> :deadletter]`
3. `[:subscriptions <id> :deadletter]`

Built-in sinks:

- `:d-core.core.messaging.dead-letter/logger`
- `:d-core.core.messaging.dead-letter/storage`
- `:d-core.core.messaging.dead-letter/producer`
- `:d-core.core.messaging.dead-letter/common` (auto-picks sinks)

Poison messages (never retried automatically):

- Codec decode failures
- Subscription schema validation failures

Consumer runtimes mark poison messages with
`[:metadata :dlq :status] = :poison` and then ACK/commit them after DLQ.

Runtime options for `send-dead-letter!`:

- `:sink` to override sink selection
- `:dlq-topic`, `:producer`, `:client` overrides for producer sink
- `:max-retries`, `:delay-ms` for producer backoff

## Deferred delivery (scheduler)

If you pass any of these options to `produce!`, D-Core schedules delivery:

- `:deliver-at-time` (#inst or java.util.Date)
- `:deliver-at-ms` (epoch ms)
- `:delay-ms` (relative ms)

Deferred delivery requires the scheduler component and a JDBC-backed Quartz
JobStore. RAMJobStore is rejected by default.

Key: `:d-core.core.messaging.deferred/scheduler`

## HTTP client (policy wrapper)

```edn
{:system
 {:d-core.core.http/clients
  {:payments {:base-url "https://api.example.com"
              :default-headers {"Accept" "application/json"}
              :policies {:rate-limit {:rate-per-sec 50 :burst 100}
                         :bulkhead {:max-concurrent 20}
                         :circuit-breaker {:failure-threshold 5}
                         :retry {:max-attempts 3}}}}}}
```

## Cache and storage (protocol use)

Protocols:

- Cache: `d-core.core.cache.protocol/CacheProtocol`
- Storage: `d-core.core.storage.protocol/StorageProtocol`

Typical usage from handlers:

```clj
(cache-lookup cache "key" {})
(cache-put cache "key" value {:ttl-ms 60000})
(storage-put storage "path/to/file" bytes {:content-type "image/png"})
```

## SQL, Datomic, search, and geo

SQL common facade:

- Key: `:d-core.core.databases.sql/common`
- Backends: `:d-core.core.databases.postgres/db`,
  `:d-core.core.databases.sqlite/db`
- Protocols: `d-core.core.databases.protocols.simple-sql/*`

Datomic:

- Key: `:d-core.core.databases.datomic/db`
- Protocol: `d-core.core.databases.protocols.datomic/DatomicProtocol`

Text search:

- Common facade: `:d-core.core.text-search/common`
- Engine: `:d-core.core.text-search.typesense/engine`
- Protocols: `d-core.core.text-search.protocol/*`

Geo:

- Index: `:d-core.core.geo.tile38/index`
- Protocol: `d-core.core.geo.protocol/GeoIndexProtocol`

## Rate limiting

Keys:

- `:d-core.core.rate-limit.sliding-window/limiter`
- `:d-core.core.rate-limit.leaky-bucket/limiter`

Protocol: `d-core.core.rate-limit.protocol/RateLimitProtocol`

## Metrics (Prometheus)

Keys:

- `:d-core.core.metrics.prometheus/registry`
- `:d-core.core.metrics.prometheus/metrics`
- `:d-core.core.metrics.prometheus/server`

Protocol: `d-core.core.metrics.protocol/MetricsProtocol`

## Time library

Key: `:d-core.libs.time/clock` (UTC by default)

Use `d-core.libs.time` to create instants/durations and avoid ad-hoc `Instant/now`.

## Cron tasks

Key: `:d-core.libs.cron-task/scheduler`

Handlers receive a context map:

```clj
{:task-id <id>
 :payload <payload>
 :deps <deps-map>
 :fire-time <java.util.Date>
 :scheduled-fire-time <Date>
 :next-fire-time <Date>
 :previous-fire-time <Date>}
```

## Retriable macro

Namespace: `d-core.libs.retriable`

Use `retriable` to wrap transient failures with custom backoff and max attempts.

## Dev/test queues

Keys:

- `:d-core.queue/in-memory-queue` (single queue)
- `:d-core.queue/in-memory-queues` (topic -> queue map)

## Implementation checklist (new feature)

1. Identify entrypoints (HTTP/GraphQL) and async flows (messaging).
2. Define topics, publish targets, and subscriptions in routing.
3. Implement handler functions in your app namespaces.
4. Wire new Integrant keys in config (clients, producers, runtimes, etc).
5. Add metrics/tracing if the feature is observable.
