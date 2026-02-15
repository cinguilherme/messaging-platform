(ns core-service.app.redis.unread-index
  (:require [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [taoensso.carmine :as car]))

(def ^:private update-last-read-lua
  (str "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
       "if (not current) or (tonumber(ARGV[2]) > tonumber(current)) then "
       "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);"
       "return 1;"
       "end;"
       "return 0;"))

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
  [{:keys [redis naming metrics]} {:keys [conversation-id message-id seq]}]
  (let [idx-key (message-index-key naming conversation-id)
        seq-key (message-seq-key naming conversation-id)
        seq-value (long seq)
        message-id-str (str message-id)]
    (app-metrics/with-redis metrics :message_index_write
      #(car/wcar (redis-lib/conn redis)
         (car/zadd idx-key seq-value message-id-str)
         (car/hset seq-key message-id-str (str seq-value))))
    {:ok true
     :index-key idx-key
     :seq-key seq-key
     :seq seq-value
     :message-id message-id-str}))

(defn message-seq
  [{:keys [redis naming metrics]} conversation-id message-id]
  (when (and redis naming conversation-id message-id)
    (let [seq-key (message-seq-key naming conversation-id)
          value (app-metrics/with-redis metrics :message_seq_read
                  #(car/wcar (redis-lib/conn redis)
                     (car/hget seq-key (str message-id))))]
      (parse-long-safe value))))

(defn update-last-read-seq!
  [{:keys [redis naming metrics]} conversation-id user-id seq]
  (when-let [seq-value (parse-long-safe seq)]
    (let [key (last-read-key naming conversation-id)
          updated? (= 1 (app-metrics/with-redis metrics :last_read_update
                            #(car/wcar (redis-lib/conn redis)
                               (car/eval update-last-read-lua
                                         1
                                         key
                                         (str user-id)
                                         (str seq-value)))))]
      {:ok true
       :updated? updated?
       :seq seq-value
       :key key})))

(defn update-last-read-by-message!
  [deps conversation-id user-id message-id]
  (when-let [seq-value (message-seq deps conversation-id message-id)]
    (update-last-read-seq! deps conversation-id user-id seq-value)))

(defn unread-count
  [{:keys [redis naming metrics]} conversation-id user-id]
  (when (and redis naming conversation-id user-id)
    (let [last-read (app-metrics/with-redis metrics :last_read_read
                      #(car/wcar (redis-lib/conn redis)
                         (car/hget (last-read-key naming conversation-id)
                                   (str user-id))))
          last-read-seq (parse-long-safe last-read)]
      (when (some? last-read-seq)
        (let [count-value (app-metrics/with-redis metrics :message_index_count
                            #(car/wcar (redis-lib/conn redis)
                               (car/zcount (message-index-key naming conversation-id)
                                           (str "(" last-read-seq)
                                           "+inf")))]
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
          (app-metrics/with-redis metrics :message_index_trim
            #(car/wcar (redis-lib/conn redis)
               (let [ids (car/zrangebyscore idx-key "-inf" end-seq)]
                 (when (seq ids)
                   (car/zremrangebyscore idx-key "-inf" end-seq)
                   (hdel-many! seq-key ids)
                   (count ids))))))
        (app-metrics/with-redis metrics :message_index_trim
          #(car/wcar (redis-lib/conn redis)
             (let [card (long (or (parse-long-safe (car/zcard idx-key)) 0))
                   remove-count (max 0 (- card keep-n))]
               (when (pos? remove-count)
                 (let [last-rank (dec remove-count)
                       ids (car/zrange idx-key 0 last-rank)]
                   (car/zremrangebyrank idx-key 0 last-rank)
                   (when (seq ids)
                     (hdel-many! seq-key ids))
                   remove-count)))))))))
