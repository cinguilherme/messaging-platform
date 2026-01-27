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
- HTTP request metrics and worker drop/error counters are instrumented.
- Remaining: Redis/Minio metrics, async consumer metrics, alerts, dashboards.

Definition of Done
- Dashboards and alert rules exist with sample thresholds.
- Runbook reviewed.

## Phase 6: Load Tests and Tuning
Deliverables
- Load test plan for 1k users x 10 msg/min and beyond.
- Tune segment size/time thresholds, Redis retention, and batch sizes.
- Document recommended default configuration for low-cost VPS.

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
