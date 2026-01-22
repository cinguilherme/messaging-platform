# ADR 00: Adopt D-Core as the Service Foundation

Status: Accepted

## Context

This service needs consistent infrastructure for messaging, HTTP, databases,
cache, storage, and observability. D-Core provides Integrant components,
protocols, and defaults for these concerns. Without a standard baseline, we
would duplicate infrastructure logic across services and drift in behavior.

## Decision

We adopt D-Core as the foundational library for this service and adhere to its
patterns and wiring. Infrastructure capabilities are provided via D-Core
components and configured through `duct.edn` (or the repo's config entrypoint)
using `#ig/ref`. Business logic, handlers, and routing decisions remain in this
repo; D-Core components should not contain domain logic. Backend variations are
chosen via configuration, not by creating parallel code paths.

## Consequences

- Infrastructure changes should be expressed as D-Core configuration and
  Integrant wiring first.
- If a required capability is missing, prefer extending D-Core or contributing
  upstream rather than adding bespoke infrastructure in this repo.
- Future ADRs and architectural choices must be compatible with D-Core's
  contracts and conventions; exceptions require an explicit ADR.

## References

- `readme-llm.md`
