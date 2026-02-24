(ns core-service.app.redis.unread-index
  (:require [core-service.app.libs.redis :as redis-lib]
            [d-core.core.state-store.protocol :as p-state]
            [d-core.core.state-store.redis :as state-store-redis]
            [core-service.app.metrics :as app-metrics]
            [taoensso.carmine :as car]))

(defn message-index-key
  [naming conversation-id]
  (let [prefix (get-in naming [:redis :message-index-prefix] "chat:msg_index:")]
    (str prefix conversation-id)))

(defn message-seq-key
  [naming conversation-id]
  (let [prefix (get-in naming [:redis :message-seq-prefix] "chat:msg_seq:")]
    (str prefix conversation-id)))

(defn last-read-key
  [naming conversation-id]
  (let [prefix (get-in naming [:redis :last-read-prefix] "chat:last_read:")]
    (str prefix conversation-id)))

(defn- parse-long-safe
  [value]
  (try
    (when (some? value)
      (Long/parseLong (str value)))
    (catch Exception _
      nil)))

(defn index-message!
  [{:keys [redis state-store naming metrics]} {:keys [conversation-id message-id seq]}]
  (let [idx-key (message-index-key naming conversation-id)
        seq-key (message-seq-key naming conversation-id)
        seq-value (long seq)
        message-id-str (str message-id)
        store (or state-store
                  (state-store-redis/->RedisStateStore redis))]
    (app-metrics/with-redis metrics :message_index_write
      #(do
         (p-state/zadd! store idx-key seq-value message-id-str nil)
         (p-state/put-field! store seq-key message-id-str (str seq-value) nil)))
    {:ok true
     :index-key idx-key
     :seq-key seq-key
     :seq seq-value
     :message-id message-id-str}))

(defn message-seq
  [{:keys [redis state-store naming metrics]} conversation-id message-id]
  (when (and redis naming conversation-id message-id)
    (let [seq-key (message-seq-key naming conversation-id)
          store (or state-store
                    (state-store-redis/->RedisStateStore redis))
          value (app-metrics/with-redis metrics :message_seq_read
                  #(p-state/get-field store seq-key (str message-id) nil))]
      (parse-long-safe value))))

(defn update-last-read-seq!
  [{:keys [redis state-store naming metrics]} conversation-id user-id seq]
  (when-let [seq-value (parse-long-safe seq)]
    (let [key (last-read-key naming conversation-id)
          store (or state-store
                    (state-store-redis/->RedisStateStore redis))
          updated? (= 1 (app-metrics/with-redis metrics :last_read_update
                          #(if (p-state/set-max-field! store key (str user-id) seq-value nil)
                             1
                             0)))]
      {:ok true
       :updated? updated?
       :seq seq-value
       :key key})))

(defn update-last-read-by-message!
  [deps conversation-id user-id message-id]
  (when-let [seq-value (message-seq deps conversation-id message-id)]
    (update-last-read-seq! deps conversation-id user-id seq-value)))

(defn unread-count
  [{:keys [redis state-store naming metrics]} conversation-id user-id]
  (when (and redis naming conversation-id user-id)
    (let [store (or state-store
                    (state-store-redis/->RedisStateStore redis))
          last-read (app-metrics/with-redis metrics :last_read_read
                      #(p-state/get-field store
                                          (last-read-key naming conversation-id)
                                          (str user-id)
                                          nil))
          last-read-seq (parse-long-safe last-read)]
      (when (some? last-read-seq)
        (let [count-value (app-metrics/with-redis metrics :message_index_count
                            #(p-state/zcount store
                                             (message-index-key naming conversation-id)
                                             (str "(" last-read-seq)
                                             "+inf"
                                             nil))]
          (or (parse-long-safe count-value) 0))))))

(defn- hdel-many!
  [key values]
  (doseq [chunk (partition-all 500 values)]
    (when (seq chunk)
      (apply car/hdel key chunk))))

(defn trim-index!
  [{:keys [redis naming metrics]} {:keys [conversation-id seq-end trim-min-entries]}]
  (when (and redis naming conversation-id)
    (let [idx-key (message-index-key naming conversation-id)
          seq-key (message-seq-key naming conversation-id)
          keep-n (long (or trim-min-entries 0))]
      (if (<= keep-n 0)
        (when-let [end-seq (parse-long-safe seq-end)]
          (let [ids (app-metrics/with-redis metrics :message_index_trim
                      #(car/wcar (redis-lib/conn redis)
                                 (car/zrangebyscore idx-key "-inf" end-seq)))]
            (when (seq ids)
              (app-metrics/with-redis metrics :message_index_trim
                #(car/wcar (redis-lib/conn redis)
                           (car/zremrangebyscore idx-key "-inf" end-seq)))
              (app-metrics/with-redis metrics :message_index_trim
                #(car/wcar (redis-lib/conn redis)
                           (hdel-many! seq-key ids)))
              (count ids))))
        (let [card (long (or (parse-long-safe
                              (app-metrics/with-redis metrics :message_index_trim
                                #(car/wcar (redis-lib/conn redis)
                                           (car/zcard idx-key))))
                             0))
              remove-count (max 0 (- card keep-n))]
          (when (pos? remove-count)
            (let [last-rank (dec remove-count)
                  ids (app-metrics/with-redis metrics :message_index_trim
                        #(car/wcar (redis-lib/conn redis)
                                   (car/zrange idx-key 0 last-rank)))]
              (app-metrics/with-redis metrics :message_index_trim
                #(car/wcar (redis-lib/conn redis)
                           (car/zremrangebyrank idx-key 0 last-rank)))
              (when (seq ids)
                (app-metrics/with-redis metrics :message_index_trim
                  #(car/wcar (redis-lib/conn redis)
                             (hdel-many! seq-key ids))))
              remove-count)))))))
