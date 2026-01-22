# Repo principles (Always apply)

## Goals
- Prefer small, reviewable diffs.
- Match existing patterns before introducing new abstractions.

## Change protocol
1) Identify the owning module/package for the change.
2) Update code + tests.
3) Run: `make test` and `make lint` (or explain why not possible).
4) If behavior changes, update docs in `AGENTS.md` (or relevant ADR).

## Don’ts
- Don't change protocols unless is not possible to do so in a backwards compatible way.
- Don’t rename public APIs without updating all call sites.
- Don’t introduce new dependencies without stating why.