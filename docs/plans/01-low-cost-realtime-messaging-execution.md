# Execution Plan: Low-Cost Real-Time Messaging PaaS

Status: In Progress (Phases 0-4 complete; Phase 5 in progress)
Source ADR: docs/adr/01-low-cost-realtime-messaging-paas.md

## Goals
- Deliver a real-time messaging API with low operational cost that fits on a
  1 vCPU / 4GB RAM VPS.
- Use Postgres for metadata only, Redis for realtime tail, and Minio for message
  history segments.
- Enforce a strict 30-day retention policy.
- Support Keycloak-backed user auth plus application-level API keys.

## Non-Goals
- Server-side full-text search.
- Infinite message durability or long-term archives beyond retention.
- Multi-region or high-availability deployments in the initial release.

## Constraints and Assumptions
- D-Core is the baseline (Integrant wiring and D-Core components).
- Workers are implemented with D-Core workers (core.async ticker workers).
- Redis is the primary realtime system; no Kafka.
- Minio is self-hosted; daily backups for Minio and Postgres go to Cloudflare R2.

## Phase 0: Contracts and API Surface
Status: Complete
Deliverables
- Message envelope schema (ids, timestamps, seq, sender, payload, attachments,
  receipt fields).
- Segment format and encoding (length-prefixed records + compression, default
  JSON; EDN/raw bytes optional).
- Pagination token format and range query semantics.
- Auth model: API key issuance/rotation, Keycloak token validation, and
  conversation membership rules.
Reference: docs/plans/01-low-cost-realtime-messaging-phase-0.md

Definition of Done
- Schemas and API contract documented and reviewed.
- Keycloak realm/client requirements defined.

## Phase 1: Infrastructure Wiring
Status: Complete
Deliverables
- D-Core config for Postgres, Redis, Minio, and worker system.
- Bucket layout and naming conventions for segments and attachments.
- Redis stream naming conventions and retention policy configuration.

Definition of Done
- Service boots in dev with Postgres/Redis/Minio.
- Health checks for all dependencies.

## Phase 2: Write Path (MVP)
Status: Complete
Deliverables
- Message ingestion endpoint with auth, validation, and rate limits.
- Sequence assignment per conversation (Redis INCR or Postgres sequence).
- Open-segment buffer in memory with size/time flush thresholds.
- Flush worker writes segment object to Minio and inserts segment index in
  Postgres.
- Realtime fanout via Redis Pub/Sub and append to Redis Streams.
- Ephemeral receipts stored in Redis only.

Definition of Done
- Messages can be sent, fanned out, and later retrieved from Redis tail.
- Segment objects appear in Minio with index rows in Postgres.

## Phase 3: Read Path (MVP)
Status: Complete
Deliverables
- History endpoint with pagination (Redis tail + Minio fallback).
- Range queries resolve to segment indices and scan segments for results.
- Websocket reconnect/catch-up using Redis Streams and pagination tokens.

Definition of Done
- Client can paginate seamlessly across Redis and Minio boundary.
- Ordering is consistent by sequence number.

## Phase 4: Retention and Cleanup
Status: Complete

Remaining Work (Phase 4)
- None.
Deliverables
- Retention worker deletes segment objects and index rows older than 30 days.
- Stream trimming aligned to retention policy.
- Receipt expiry aligned to Redis retention.

Definition of Done
- Retention jobs run on schedule and reduce storage within expected bounds.

## Phase 5: Observability and Ops
Status: In Progress
Deliverables
- Metrics: flush latency, stream depth, segment sizes, Redis memory, Minio
  error rates, delivery latency.
- HTTP request metrics (latency, status counts, error rate) and auth failures.
- Async messaging/worker metrics (queue depth, handler latency, error counts).
- Expose Prometheus-compatible `/metrics` endpoint for scraping; Grafana
  dashboards for core KPIs.
- Alerts: flush backlog, Redis memory pressure, Minio error spikes.
- Operational runbook (how to recover, how to tune thresholds).

Notes / Gaps
- Prometheus registry + `/metrics` endpoint are now wired via D-Core metrics.
- HTTP request metrics, worker drop/error counters, consumer handler metrics,
  and Redis/Minio operation metrics are instrumented.
- Remaining: async consumer queue/lag metrics, alerts, dashboards.

Definition of Done
- Dashboards and alert rules exist with sample thresholds.
- Runbook reviewed.

## Phase 5b: Debug Logging and Traceability (Revision)
Status: Planned
Deliverables
- Component-level debug logs for all async workers (flush, retention, fanout,
  consumer handlers).
- Tick logs for ticker-based workers (schedule interval, last/next tick time).
- Structured log fields: component, worker, conversation-id, segment-id,
  seq-range, duration-ms, error.
- Explicit success/failure logs around segment flush stages (buffer snapshot,
  encode, Minio put, Postgres index write).
- Log sampling/throttling rules for high-frequency paths; defaults documented.

Draft: Log Schema / Fields (structured)
- timestamp, level, message
- component (e.g., "segment-flush", "retention", "fanout", "consumer")
- worker (name/id), tick-id, tick-scheduled-at, tick-started-at, tick-ended-at
- conversation-id, segment-id, seq-start, seq-end, message-count, payload-bytes
- stage (buffer-snapshot|encode|minio-put|index-write|publish|trim)
- duration-ms, retry-count, attempt, success (true/false)
- error.kind, error.message, error.stack (when error)
- storage.bucket, storage.key (for Minio ops)
- pg.table, pg.operation, pg.rows (for index writes)
- redis.stream, redis.op, redis.bytes (for stream/publish ops)
- sampled (true/false), sample-rate

Draft: Config Toggles
- :observability :logging :level (info|debug)
- :observability :logging :components (set of component names)
- :observability :logging :workers (set of worker ids)
- :observability :logging :segment-flush :enabled (true/false)
- :observability :logging :segment-flush :stage-logs (true/false)
- :observability :logging :ticker :enabled (true/false)
- :observability :logging :ticker :tick-log-interval-ms
- :observability :logging :sampling :enabled (true/false)
- :observability :logging :sampling :default-rate
- :observability :logging :sampling :per-component (map of component->rate)
- :observability :logging :redact-fields (set of field names)
- :observability :logging :include-stacktraces (true/false)

Sample Log Lines (segment flush)
```
{"ts":"2026-01-28T14:02:10.120Z","level":"debug","message":"segment flush tick start","component":"segment-flush","worker":"segment-flush-1","tick-id":"c2c8d1","tick-scheduled-at":"2026-01-28T14:02:10.000Z","tick-started-at":"2026-01-28T14:02:10.120Z","conversation-id":"3e3b0b0a-7cbb-4c6b-9b1c-64f5413c75b2","message-count":42}
{"ts":"2026-01-28T14:02:10.134Z","level":"debug","message":"segment flush stage ok","component":"segment-flush","worker":"segment-flush-1","tick-id":"c2c8d1","stage":"encode","duration-ms":9,"seq-start":901,"seq-end":942,"payload-bytes":18432}
{"ts":"2026-01-28T14:02:10.162Z","level":"info","message":"segment flush stage ok","component":"segment-flush","worker":"segment-flush-1","tick-id":"c2c8d1","stage":"minio-put","duration-ms":18,"storage.bucket":"segments","storage.key":"conv/3e3b0b0a/seg/901-942.msgpack","payload-bytes":18432}
{"ts":"2026-01-28T14:02:10.190Z","level":"info","message":"segment flush stage ok","component":"segment-flush","worker":"segment-flush-1","tick-id":"c2c8d1","stage":"index-write","duration-ms":21,"pg.table":"segments","pg.operation":"insert","pg.rows":1}
{"ts":"2026-01-28T14:02:10.202Z","level":"info","message":"segment flush tick complete","component":"segment-flush","worker":"segment-flush-1","tick-id":"c2c8d1","tick-ended-at":"2026-01-28T14:02:10.202Z","duration-ms":82,"conversation-id":"3e3b0b0a-7cbb-4c6b-9b1c-64f5413c75b2","segment-id":"seg-901-942","seq-start":901,"seq-end":942,"message-count":42,"payload-bytes":18432,"success":true}
```

Sample Log Lines (segment flush failures)
```
{"ts":"2026-01-28T14:05:10.120Z","level":"warn","message":"segment flush tick start (empty buffer)","component":"segment-flush","worker":"segment-flush-1","tick-id":"7c1a9e","tick-scheduled-at":"2026-01-28T14:05:10.000Z","tick-started-at":"2026-01-28T14:05:10.120Z","message-count":0,"success":true}
{"ts":"2026-01-28T14:06:10.144Z","level":"error","message":"segment flush stage failed","component":"segment-flush","worker":"segment-flush-1","tick-id":"ab901f","stage":"minio-put","duration-ms":31,"storage.bucket":"segments","storage.key":"conv/3e3b0b0a/seg/943-980.msgpack","error.kind":"minio.error","error.message":"Timeout waiting for response","retry-count":1,"success":false}
{"ts":"2026-01-28T14:06:10.145Z","level":"error","message":"segment flush tick failed","component":"segment-flush","worker":"segment-flush-1","tick-id":"ab901f","tick-ended-at":"2026-01-28T14:06:10.145Z","duration-ms":44,"conversation-id":"3e3b0b0a-7cbb-4c6b-9b1c-64f5413c75b2","seq-start":943,"seq-end":980,"message-count":38,"payload-bytes":16920,"success":false,"error.kind":"minio.error","error.message":"Timeout waiting for response"}
```

Notes / Gaps
- Load tests show missing visibility for segment flush activity.
- Need to disambiguate "never ticks" vs "ticks but fails silently."

Definition of Done
- Segment flush emits clear start/finish logs per tick and on failure paths.
- Ticker workers log when they start and when ticks are skipped/backlogged.
- Debug logs are enabled via config without code changes.

## Phase 6: Load Tests and Tuning
Deliverables
- Load test plan for 1k users x 10 msg/min and beyond.
- Simple CLI load runner for local/dev to generate traffic and observe metrics.
- Tune segment size/time thresholds, Redis retention, and batch sizes.
- Document recommended default configuration for low-cost VPS.

CLI (local/dev)
```
clojure -M -m core-service.dev.load-cli api-key --write true
clojure -M -m core-service.dev.load-cli login --base-url http://localhost:3000 --api-key $KEY --user 1
clojure -M -m core-service.dev.load-cli token --direct true --user 1
clojure -M -m core-service.dev.load-cli whoami --token $TOKEN
clojure -M -m core-service.dev.load-cli create-conversation --base-url http://localhost:3000 --api-key $KEY --token $TOKEN
clojure -M -m core-service.dev.load-cli send --base-url http://localhost:3000 --api-key $KEY --token $TOKEN --conversation-id <uuid> --count 100 --delay-ms 10
clojure -M -m core-service.dev.load-cli load --base-url http://localhost:3000 --api-key $KEY --token $TOKEN --conversation-id <uuid> --rate 10 --duration-s 60
```

Definition of Done
- Target load runs within CPU/memory limits on 1 vCPU / 4GB RAM.
- Default configuration documented.

## Phase 7: Hardening and Optional Scale
Deliverables
- Idempotency keys for ingest to prevent duplicates.
- Backfill/recovery worker for gaps between Redis and Minio.
- Optional async queue backend integration (Redis queues/RabbitMQ).

Definition of Done
- Retry paths produce no duplicate messages.
- Recovery worker validated with fault injection tests.

## Milestone Checklist
- Contracts finalized and reviewed.
- MVP write path done.
- MVP read path done.
- Retention and backups running.
- Load tests pass on target VPS.
- Operational documentation complete.
