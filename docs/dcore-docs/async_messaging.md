# Async Messaging

This document describes how to wire async messaging in Duct with explicit
publish targets and decoupled consumer subscriptions.

## Design goals

- Explicit publish targets (no implicit fanout).
- Consumers only start from `:subscriptions` (publish config never creates consumers).
- Multi-cluster friendly (multiple client instances per transport).
- Clear separation: `:topics` (schema/metadata) vs `:publish` vs `:subscriptions`.

## Routing shape

```edn
{:topics
 {:order-created {:schema {:canonical [:map {:closed true} [:id :uuid] [:amount :double]]}
                  :relationship :both}}

 :publish
 {:order-created {:targets [{:producer :kafka-primary
                              :kafka-topic "core.orders"}
                             {:producer :kafka-data
                              :kafka-topic "core.orders.data"}]}}

 :subscriptions
 {:orders-worker {:topic :order-created
                  :source :kafka
                  :client :kafka-primary
                  :handler :my-app.handlers/order-created
                  :options {:group-id "orders-core"}}
  :orders-data-worker {:topic :order-created
                       :source :kafka
                       :client :kafka-data
                       :handler :my-app.handlers/order-created-data
                       :options {:group-id "orders-data"}}}}
```

Notes:
- `:topics` is shared metadata (schema, naming hints, etc).
- `:publish` is only used by producers. Each target can include per-target
  overrides (e.g. `:kafka-topic`, `:stream`, `:subject`).
- `:subscriptions` is the only source of consumers.
- `:client` is the cluster key. `:producer` is accepted as an alias for
  backward-compatibility.

## Producer fanout

Producers use `:publish` targets. One produce call can fan out to multiple
targets:

```clojure
(producer/produce! producer {:id 1} {:topic :order-created})
```

Targets are resolved in order. If any target fails, the call fails and the
whole produce is retried by your caller.

## Deferred delivery (scheduler-backed)

Producers accept deferred delivery options. When `:deliver-at-*` or `:delay-ms`
is provided, D-core schedules the publish for later instead of publishing now.

Options (resolution order):

- `:deliver-at-time` (java.util.Date / #inst)
- `:deliver-at-ms` (epoch ms)
- `:delay-ms` (relative delay in ms)

Example:

```clojure
(producer/produce! producer {:id 1}
                   {:topic :order-created
                    :deliver-at-time #inst "2025-01-01T10:00:00.000-00:00"})
```

To enable deferred delivery, wire the scheduler component and pass it into the
common producer. Deferred scheduling requires Quartz JDBCJobStore configuration
(RAMJobStore is rejected by default).

```clojure
{:system
 {:d-core.core.messaging.deferred/scheduler
  {:producers #ig/ref :d-core.core.producers/registry
   :quartz {:properties {"org.quartz.threadPool.threadCount" "4"
                         "org.quartz.jobStore.class" "org.quartz.impl.jdbcjobstore.JobStoreTX"
                         "org.quartz.jobStore.isClustered" "true"
                         "org.quartz.jobStore.driverDelegateClass" "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate"
                         "org.quartz.jobStore.dataSource" "main"
                         "org.quartz.dataSource.main.connectionProvider.class" "org.quartz.utils.HikariCpPoolingConnectionProvider"
                         "org.quartz.dataSource.main.driver" "org.postgresql.Driver"
                         "org.quartz.dataSource.main.URL" "jdbc:postgresql://localhost:5432/d-core"
                         "org.quartz.dataSource.main.user" "postgres"
                         "org.quartz.dataSource.main.password" "postgres"
                         "org.quartz.dataSource.main.maxConnections" "5"}}}

  :d-core.core.producers.common/producer
  {:producers #ig/ref :d-core.core.producers/registry
   :routing #ig/ref :d-core.core.messaging/routing
   :deferred #ig/ref :d-core.core.messaging.deferred/scheduler}}}
```

If the scheduler component is not configured, deferred options will raise an
error rather than silently publishing immediately.

Notes:
- Deferred scheduling requires JDBCJobStore; RAMJobStore is rejected.
- With JDBCJobStore, the deferred payload and options must be Java-serializable.

## Consumer wiring

Consumer runtimes filter subscriptions by `:source` only. The client instance
is chosen by `:client` (or `:producer`):

```edn
{:subscriptions
 {:orders-worker {:topic :order-created
                  :source :kafka
                  :client :kafka-primary
                  :handler :my-app.handlers/order-created}}}
```

No subscription is started for publish-only topics unless explicitly listed in
`:subscriptions`.

### Subscription handler resolution

` :d-core.core.messaging/routing` resolves handlers during Integrant init:

- `:handler` may be a function or a keyword.
- If a keyword is provided, it is looked up in `:handlers`.
- All resolved handlers are wrapped with tracing (`:metadata :trace` is decoded
  and bound to `d-core.tracing/*ctx*` for the duration of the handler).

You can supply routing in two ways:

1) **Plain routing map** (existing behavior):

```edn
{:my-app.config.messaging/routing
 {:handlers {:orders-handler #ig/ref :my-app.handlers/orders}
  :topics {:orders {}}
  :subscriptions {:orders-worker {:topic :orders
                                  :source :kafka
                                  :client :kafka-primary
                                  :handler :orders-handler}}}

 :d-core.core.messaging/routing #ig/ref :my-app.config.messaging/routing}
```

2) **Default routing + overrides** (new, deep-merge friendly):

```edn
{:my-app.config.messaging/routing
 {:default-routing {:topics {:orders {}}
                    :subscriptions {:orders-worker {:topic :orders
                                                    :source :kafka
                                                    :handler :orders-handler}}}
  :overrides {:handlers {:orders-handler #ig/ref :my-app.handlers/orders}
              ;; override any part of routing (subscriptions, topics, defaults, etc)
              :subscriptions {:orders-worker {:client :kafka-primary}}}}

 :d-core.core.messaging/routing #ig/ref :my-app.config.messaging/routing}
```

Notes:
- `:default-routing` and `:overrides` are deep-merged; non-map values on the
  right overwrite.
- `:handlers` are merged from both maps; a keyword handler can live in either.

## Multi-cluster clients

Clients are named by the application developer. Example Kafka clients:

```edn
:d-core.core.clients/kafka
{:kafka-primary {:bootstrap-servers "localhost:29092"}
 :kafka-data {:bootstrap-servers "localhost:9094"}}
```

Your producer registry should expose one producer per client key:

```edn
:d-core.core.producers/registry
{:kafka-primary #ig/ref :d-core.core.producers.kafka/primary
 :kafka-data #ig/ref :d-core.core.producers.kafka/data}
```

## Dead letters

DLQ config merges in this order:

1. `[:defaults :deadletter]`
2. `[:topics <topic> :deadletter]`
3. `[:subscriptions <id> :deadletter]`

Consumers attach the subscription’s `:client` to `[:metadata :dlq :producer]`.
The producer DLQ sink uses that key to publish retries on the same cluster by
default.

## Integrant wiring example

```edn
{:system
 {:my-app.config.messaging/routing
  {:topics {:order-created {}}
   :publish {:order-created {:targets [{:producer :kafka-primary
                                        :kafka-topic "core.orders"}
                                       {:producer :kafka-data
                                        :kafka-topic "core.orders.data"}]}}
   :subscriptions {:orders-worker {:topic :order-created
                                   :source :kafka
                                   :client :kafka-primary
                                   :handler :my-app.handlers/order-created}}}

  :d-core.core.messaging/routing #ig/ref :my-app.config.messaging/routing
  ;; ... clients/producers/consumers ...
  }}
```
