(ns core-service.app.redis.conversation-last
  (:require [clojure.edn :as edn]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [taoensso.carmine :as car]))

(def ^:private update-last-message-lua
  (str "local current = redis.call('HGET', KEYS[1], 'seq');"
       "if (not current) or (tonumber(ARGV[1]) > tonumber(current)) then "
       "redis.call('HSET', KEYS[1], 'seq', ARGV[1], 'payload', ARGV[2]);"
       "return 1;"
       "end;"
       "return 0;"))

(defn last-message-key
  [naming conversation-id]
  (let [prefix (get-in naming [:redis :conversation-last-prefix] "chat:conv_last:")]
    (str prefix conversation-id)))

(defn- parse-payload
  [payload]
  (try
    (cond
      (bytes? payload) (edn/read-string (String. ^bytes payload "UTF-8"))
      (string? payload) (edn/read-string payload)
      :else nil)
    (catch Exception _
      nil)))

(defn update-last-message!
  [{:keys [redis naming metrics]} {:keys [conversation_id seq] :as message}]
  (when (and redis naming conversation_id (some? seq))
    (let [key (last-message-key naming conversation_id)
          seq-value (long seq)
          payload (pr-str message)
          updated? (= 1 (app-metrics/with-redis metrics :conversation_last_write
                          #(car/wcar (redis-lib/conn redis)
                                     (car/eval update-last-message-lua
                                               1
                                               key
                                               (str seq-value)
                                               payload))))]
      {:ok true
       :updated? updated?
       :seq seq-value
       :key key})))

(defn batch-last-messages
  [{:keys [redis naming metrics]} conversation-ids]
  (let [conversation-ids (->> conversation-ids (remove nil?) vec)]
    (if (or (empty? conversation-ids) (nil? redis) (nil? naming))
      {}
      (let [keys (mapv #(last-message-key naming %) conversation-ids)
            payloads (app-metrics/with-redis metrics :conversation_last_read_batch
                       #(car/wcar (redis-lib/conn redis)
                                  (mapv (fn [k] (car/hget k "payload")) keys)))
            payloads (cond
                       (sequential? payloads) payloads
                       (and (= 1 (count conversation-ids)) (some? payloads)) [payloads]
                       :else (repeat (count conversation-ids) nil))]
        (reduce-kv (fn [acc idx conv-id]
                     (if-let [message (parse-payload (nth payloads idx nil))]
                       (assoc acc conv-id message)
                       acc))
                   {}
                   conversation-ids)))))
