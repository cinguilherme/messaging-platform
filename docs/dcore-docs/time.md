## Time

This library standardizes time handling across D-Core while keeping Java's
`java.time` API under the hood. The goal is:

- Use a single `Clock` component in the system (UTC by default).
- Allow ergonomics for instants and durations without Java interop noise.
- Accept instants as data maps or literals when moving through boundaries
  (messages, HTTP payloads, or configs).

### Core ideas

- A `Clock` is the source of "now". Pass it through your system and prefer
  `time/now` or `time/plus` on that clock rather than `Instant/now`.
- Instants can be passed as `Instant`, epoch millis, `#inst` literals, or a
  map shape like `{:epoch-ms 1710000000000}`.
- Maps are the "data" representation. Use `instant->map`/`map->instant`.

### Clock component (Integrant/Duct)

This lib provides a default Clock component so the system is always UTC.

Example config:

```clj
{:d-core.libs.time/clock {}}
```

Optional overrides:

```clj
{:d-core.libs.time/clock
 {:type :fixed
  :instant #inst "2024-01-01T00:00:00.000-00:00"
  :zone "UTC"}}
```

Supported types:
- `:system` (default)
- `:fixed`
- `:offset`
- `:tick`

### Usage

```clj
(require '[d-core.libs.time :as time])

;; system clock (UTC via component)
(def clock (time/new-clock {:type :system :zone "UTC"}))

(time/now clock)
(time/now-map clock)            ;; => {:epoch-ms ... :zone "UTC"}

(time/plus clock :minutes 5)
(time/minus #inst "2024-01-01T00:00:30.000-00:00" :seconds 10)

(time/since clock {:epoch-ms 1704067200000})
(time/duration {:seconds 1 :millis 250})

(time/before? #inst "2024-01-01T00:00:00.000-00:00"
              #inst "2024-01-01T00:00:10.000-00:00")
```

### Data shapes

Instant maps:
- `{:epoch-ms <long>}`
- `{:epoch-second <long> :nano <int>}`

Duration maps:
- `{:millis <long>}` (for output)
- `{:seconds 1 :millis 250}` (for input)

### Guidance

- Always pass a `Clock` from the system into business logic when "now" matters.
- Use `#inst` for literals in config/tests; use maps at boundaries.
- Convert to maps at the edges, keep `Instant` internally.
