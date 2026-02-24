(ns core-service.app.server.receipt.logic
  (:require [core-service.app.libs.identity :as identity]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.libs.util :as util]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.server.http :as http]
            [taoensso.carmine :as car]))

(defn sender-id-from-request
  [req]
  (identity/user-id-from-request req))

(defn coerce-receipt-create
  [data]
  (cond-> data
    (contains? data :receipt_type)
    (update :receipt_type util/coerce-keyword)
    (contains? data :message_id)
    (update :message_id http/parse-uuid)
    (contains? data :at)
    (update :at (fn [v] (when (some? v) (http/parse-long v nil))))))

(defn receipt-read?
  [redis metrics naming conversation-id message-id user-id]
  (let [key (receipts/receipt-key naming conversation-id message-id)
        field (str "read:" user-id)]
    (app-metrics/with-redis metrics :hget
      #(car/wcar (redis-lib/conn redis)
         (car/hget key field)))))

(defn batch-receipt-read?
  "Check read receipts for multiple messages in a single Redis pipeline.
  Returns a map of message-id -> receipt-value (nil when not read)."
  [redis metrics naming conversation-id message-ids user-id]
  (if (empty? message-ids)
    {}
    (let [keys-and-fields (mapv (fn [mid]
                                  [(receipts/receipt-key naming conversation-id mid)
                                   (str "read:" user-id)])
                                message-ids)
          results (app-metrics/with-redis metrics :hget_batch
                    #(car/wcar (redis-lib/conn redis)
                       (mapv (fn [[k f]] (car/hget k f)) keys-and-fields)))]
      (zipmap message-ids results))))
