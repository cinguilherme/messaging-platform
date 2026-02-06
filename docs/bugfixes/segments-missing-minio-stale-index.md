# Bugfix: Stale Segment Index After Minio TTL

## Summary
When a conversation’s Redis stream expires and Minio segment objects are removed by TTL, the `segment_index` table can still reference deleted objects. The conversations list endpoint reads the last message from Redis or Minio; with Redis empty and Minio objects missing, the reader repeatedly attempts to fetch missing objects. This led to timeouts/no response in the conversation list and blocked UX even though conversations are correctly persisted in Postgres.

## Root Cause
`segment_index` rows were not pruned when the referenced Minio object no longer existed. The Minio reader treated missing objects as generic failures and simply returned no messages without cleaning the index. Subsequent requests kept hitting the same stale rows, causing repeated failed Minio gets and request stalls when Redis had already expired.

## Fix
1. Detect Minio “not found” errors in `minio/get-bytes!` and surface them as `:error-type :not-found`.
2. In `segments.reader/fetch-messages`, treat missing objects as a signal to delete the stale `segment_index` row and continue the scan.
3. Add tests to cover the stale-index scenario and ensure conversations list returns successfully with `last_message` unset when both Redis and Minio are empty.

## Tests Added
- Unit: `core-service.unit.segments-reader-test/fetch-messages-prunes-missing-segment`
- Integration: `core-service.integration.conversations-list-test/conversations-list-missing-minio-segment`

## Verification
- `INTEGRATION=1 make tests`

## Expected Behavior After Fix
- Conversations list remains responsive even after Redis streams expire and Minio segment objects are deleted.
- Stale segment index rows are automatically removed on read, preventing repeated missing-object lookups.
