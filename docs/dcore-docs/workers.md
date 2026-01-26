# Workers (core.async)

This document describes the experimental workers runtime built on
`clojure.core.async`.

The workers runtime is intentionally small:
- D-core owns the entrypoints and wiring.
- Application code owns the topology and business logic.

## Concepts

- **Channels**: named core.async channels with explicit buffering rules.
- **Workers**: go or thread loops that consume from a channel and invoke a
  worker function.
- **Entry points**: exposed command workers that accept inputs from other parts
  of the system (HTTP handlers, consumers, etc).
- **Outputs**: optional output channel for worker results.
- **Failures**: optional fail channel that receives structured error events.

## Design Constraints

- **Single-consumer outputs**: `:output-chan` is designed for a single consumer.
  If you need fan-out, compose it explicitly with core.async (`mult`, `pub`) in
  app code.
- **Best-effort outputs**: output delivery uses `offer!` and can drop when the
  output channel is full. Drops emit `:workers.drop` events.
- **Input put-mode is explicit**: `:put-mode` controls how `command!` writes to
  exposed channels. Use `:block` only from normal threads.
- **Go blocks must not block**: use `:dispatch :go` only for non-blocking
  handlers. Blocking calls should run in `:dispatch :thread`.

## Config Shape

```edn
{:channels {:commands/in {:buffer 8}
            :images/in {:buffer 4 :buffer-type :dropping :put-mode :drop}
            :images/resize {:buffer 2}
            :images/out {:buffer 4}
            :workers/errors {:buffer 8}}
 :workers {:commands {:kind :command
                      :in :commands/in
                      :worker-fn my-app.workers/commands
                      :dispatch :thread
                      :expose? true
                      :fail-chan :workers/errors}
           :image-download {:kind :command
                            :in :images/in
                            :worker-fn my-app.workers/image-download
                            :dispatch :thread
                            :expose? true}
           :image-resize {:kind :command
                          :in :images/resize
                          :worker-fn my-app.workers/image-resize
                          :dispatch :thread}
           :image-store {:kind :command
                         :in :images/store
                         :worker-fn my-app.workers/image-store
                         :output-chan :images/out
                         :dispatch :thread}}}
```

### Channels

- `:buffer` (default `0`) sets the buffer size.
- `:buffer-type` (default `:fixed`) can be:
  - `:fixed` (backpressure when full)
  - `:dropping` (drop new messages when full)
  - `:sliding` (drop oldest when full)
- `:put-mode` (default `:async`) controls how `command!` writes:
  - `:async` -> non-blocking `put!`
  - `:block` -> blocking `>!!` (only use from a normal thread)
  - `:drop` -> non-blocking `offer!` with a drop event

### Workers

Common keys:
- `:kind` (`:ticker` | `:command`)
- `:dispatch` (`:go` | `:thread`, default `:go`)
- `:worker-fn` (function reference)
- `:expose?` (only for `:command`, exposes a public entry point)
- `:output-chan` (optional output channel for results)
- `:fail-chan` (optional error channel for structured failures)

## Runtime API (libs/workers)

```clojure
(require '[d-core.libs.workers :as workers])

(def system
  (workers/start-workers definition components
                         {:dev-guard? true
                          :guard-ms 50}))

(workers/command! system :image-download {:image-url "..."})
(async/<!! (workers/request! system :image-download {:image-url "..."}))
(workers/stats-snapshot system)

  ((:stop! system))
```

## Integrant Component

```edn
{:my-app.config.workers/definition
 {:channels {:commands/in {:buffer 8}}
  :workers {:commands {:kind :command
                       :in :commands/in
                       :worker-fn :my-app.workers/commands
                       :dispatch :thread
                       :expose? true}}}

 :d-core.core.workers/system
 {:definition #ig/ref :my-app.config.workers/definition
  :components {:obs #ig/ref :my-app.observability/component}
  :dev-guard? true
  :guard-ms 50}}
```

## Quickstart

```clojure
(require '[clojure.core.async :as async]
         '[d-core.libs.workers :as workers])

(def definition
  {:channels {:commands/in {:buffer 8}
              :workers/errors {:buffer 8}}
   :workers {:commands {:kind :command
                        :in :commands/in
                        :worker-fn (fn [_ msg] (println "cmd" msg))
                        :dispatch :thread
                        :expose? true
                        :fail-chan :workers/errors}}})

(def system (workers/start-workers definition {} {:dev-guard? true}))
(workers/command! system :commands {:cmd :ping})
((:stop! system))
```

### Worker Context

Worker functions receive `(ctx msg)` where `ctx` includes:
- `:components` (Integrant components)
- `:channels` (resolved channels map)
- `:worker` (worker metadata)
- `:stats` (drops/errors counters)
- `:emit` (observability emitter if provided)

## Observability

If `components` includes `:observability`, `:obs`, or `:logger` the runtime
will attempt to resolve an emitter function and send events:

- `:workers.drop` when a message is dropped
- `:workers.error` when a worker throws

If `:fail-chan` is configured, errors are also forwarded into that channel.

## Examples

### 1) Exposed command with replies

```clojure
(def reply (workers/request! system :image-download {:image-url "..." }))
(async/<!! reply)
```

### 2) Output channel consumer

```clojure
(async/go-loop []
  (when-some [event (async/<! (get-in system [:channels :images/out]))]
    (println "image event:" event)
    (recur)))
```

### 3) Error channel consumer

```clojure
(async/go-loop []
  (when-some [event (async/<! (get-in system [:channels :workers/errors]))]
    (println "worker error event:" (dissoc event :error))
    (recur)))
```
