# Dead Letters

Dead letters capture messages that failed processing so they can be inspected and replayed later.

## Public API

Namespace: `d-core.core.messaging.dead-letter`

- `DeadLetterProtocol`
- `send-dead-letter!`
- `normalize-error-info`

The protocol itself lives in `d-core.core.messaging.dead-letter.protocol` and is re-exported by the facade.

## Integrant components (built-ins)

All built-in components keep their existing Integrant keys:

- `:d-core.core.messaging.dead-letter/logger`
- `:d-core.core.messaging.dead-letter/storage`
- `:d-core.core.messaging.dead-letter/producer`
- `:d-core.core.messaging.dead-letter/common`

Implementation namespaces:

- Logger sink: `d-core.core.messaging.dead-letter.sinks.logger`
- Storage sink: `d-core.core.messaging.dead-letter.sinks.storage`
- Producer sink: `d-core.core.messaging.dead-letter.sinks.producer`
- Common facade: `d-core.core.messaging.dead-letter.common`

## Runtime options (`opts`)

`send-dead-letter!` takes an `opts` map. Common keys:

- `:sink`: override the sink when using the common facade (`:logger`, `:storage`, `:producer`, or your custom sink key)
- `:dlq-topic`: override producer destination topic
- `:producer` / `:client`: override which producer/client key publishes the DLQ message
- `:max-retries`: override producer max retries
- `:delay-ms`: override producer delay before publishing

Storage sinks may also accept backend-specific put options (forwarded to `storage-put`).

## Poison messages (non-retriable)

Some failures are *not* transient and should **never** be replayed automatically:

- Undecodable payloads (codec failures)
- Subscription schema validation failures

These are treated as **poison**:

- They are sent to the DLQ with `[:metadata :dlq :status]` set to `:poison`
- Consumer runtimes **ACK/commit** them after DLQ so they don't loop forever
- DLQ replay skips anything with status not `:eligible`

## Routing behavior

DLQ publishing uses the producer/client key captured at the time of consumption:

- Consumer runtimes enrich envelopes with `[:metadata :dlq :producer]`.
- Producer DLQ sink uses that key by default, unless overridden by `:producer`/`:client` in opts.
- Dead letter config merges in this order: defaults -> topic -> subscription.

Internally, poison classification uses `error-info` `:failure/type` values:

- `:codec-decode-failed`
- `:schema-invalid`

## Extending

To add a new sink:

1. Create a namespace under `d-core.core.messaging.dead-letter.sinks.*`
2. Implement `DeadLetterProtocol`
3. (Optional) Define an Integrant `ig/init-key` for wiring
4. Ensure the namespace is required at runtime (requiring `d-core.core.messaging.dead-letter` will load built-ins; custom sinks should be required by your app/module)
