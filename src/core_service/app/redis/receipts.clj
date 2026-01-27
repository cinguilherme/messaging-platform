(ns core-service.app.redis.receipts
  (:require [core-service.app.libs.redis :as redis-lib]
            [taoensso.carmine :as car]))

(defn receipt-key
  [naming conversation-id message-id]
  (let [prefix (get-in naming [:redis :receipts-prefix] "chat:receipts:")]
    (str prefix conversation-id ":" message-id)))

(defn record!
  [{:keys [redis naming receipt]} {:keys [conversation-id message-id user-id receipt-type at]}]
  (let [key (receipt-key naming conversation-id message-id)
        field (str (name receipt-type) ":" user-id)
        value (str (or at (System/currentTimeMillis)))
        ttl-ms (long (or (:ttl-ms receipt) 0))]
    (car/wcar (redis-lib/conn redis)
      (car/hset key field value)
      (when (pos? ttl-ms)
        (car/pexpire key ttl-ms)))
    {:ok true :key key}))

(defn get-receipts
  [{:keys [redis naming]} {:keys [conversation-id message-id]}]
  (let [key (receipt-key naming conversation-id message-id)
        entries (car/wcar (redis-lib/conn redis)
                  (car/hgetall key))]
    (apply hash-map entries)))
