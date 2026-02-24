(ns core-service.app.redis.receipts
  (:require [d-core.core.state-store.protocol :as p-state]
            [d-core.core.state-store.redis :as state-store-redis]
            [core-service.app.metrics :as app-metrics]))

(defn receipt-key
  [naming conversation-id message-id]
  (let [prefix (get-in naming [:redis :receipts-prefix] "chat:receipts:")]
    (str prefix conversation-id ":" message-id)))

(defn record!
  [{:keys [redis state-store naming receipt metrics]} {:keys [conversation-id message-id user-id receipt-type at]}]
  (let [key (receipt-key naming conversation-id message-id)
        field (str (name receipt-type) ":" user-id)
        value (str (or at (System/currentTimeMillis)))
        ttl-ms (long (or (:ttl-ms receipt) 0))
        store (or state-store
                  (state-store-redis/->RedisStateStore redis))]
    (app-metrics/with-redis metrics :receipt_write
      #(p-state/put-field! store key field value {:ttl-ms ttl-ms}))
    {:ok true :key key}))

(defn get-receipts
  [{:keys [redis state-store naming metrics]} {:keys [conversation-id message-id]}]
  (let [key (receipt-key naming conversation-id message-id)
        store (or state-store
                  (state-store-redis/->RedisStateStore redis))]
    (app-metrics/with-redis metrics :hgetall
      #(p-state/get-all store key nil))))

(defn batch-get-receipts
  [{:keys [redis state-store naming metrics]} {:keys [conversation-id message-ids]}]
  (if (empty? message-ids)
    {}
    (let [store (or state-store
                    (state-store-redis/->RedisStateStore redis))]
      (reduce (fn [acc message-id]
                (let [key (receipt-key naming conversation-id message-id)
                      entries (app-metrics/with-redis metrics :hgetall_batch
                                #(p-state/get-all store key nil))]
                  (assoc acc message-id (or entries {}))))
              {}
              message-ids))))
