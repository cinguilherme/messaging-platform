(ns core-service.app.redis.receipts
  (:require [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [taoensso.carmine :as car]))

(defn receipt-key
  [naming conversation-id message-id]
  (let [prefix (get-in naming [:redis :receipts-prefix] "chat:receipts:")]
    (str prefix conversation-id ":" message-id)))

(defn record!
  [{:keys [redis naming receipt metrics]} {:keys [conversation-id message-id user-id receipt-type at]}]
  (let [key (receipt-key naming conversation-id message-id)
        field (str (name receipt-type) ":" user-id)
        value (str (or at (System/currentTimeMillis)))
        ttl-ms (long (or (:ttl-ms receipt) 0))]
    (app-metrics/with-redis metrics :receipt_write
      #(car/wcar (redis-lib/conn redis)
         (car/hset key field value)
         (when (pos? ttl-ms)
           (car/pexpire key ttl-ms))))
    {:ok true :key key}))

(defn get-receipts
  [{:keys [redis naming metrics]} {:keys [conversation-id message-id]}]
  (let [key (receipt-key naming conversation-id message-id)
        entries (app-metrics/with-redis metrics :hgetall
                  #(car/wcar (redis-lib/conn redis)
                     (car/hgetall key)))]
    (apply hash-map entries)))

(defn batch-get-receipts
  [{:keys [redis naming metrics]} {:keys [conversation-id message-ids]}]
  (if (empty? message-ids)
    {}
    (let [keys (mapv #(receipt-key naming conversation-id %) message-ids)
          entries-by-key (app-metrics/with-redis metrics :hgetall_batch
                          #(car/wcar (redis-lib/conn redis)
                             (mapv car/hgetall keys)))
          entries-by-key (if (sequential? entries-by-key)
                           entries-by-key
                           [])]
      (reduce-kv
       (fn [acc idx message-id]
         (let [entries (nth entries-by-key idx nil)]
           (assoc acc message-id
                  (if (and entries (sequential? entries))
                    (apply hash-map entries)
                    {}))))
       {}
       message-ids))))
