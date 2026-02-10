(ns core-service.app.server.message.logic
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.pagination :as pagination]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.server.http :as http]
            [core-service.app.streams.redis :as streams]
            [duct.logger :as logger]
            [taoensso.carmine :as car]))

(defn sender-id-from-request
  [req]
  (or (http/parse-uuid (get-in req [:auth/principal :subject]))
      (http/parse-uuid (get-in req [:auth/principal :user_id]))))

(defn next-seq!
  [redis-client metrics key]
  (app-metrics/with-redis metrics :incr
    #(car/wcar (redis-lib/conn redis-client)
       (car/incr key))))

(defn coerce-message-create
  [data]
  (-> data
      (update :type (fn [v] (if (string? v) (keyword v) v)))))

(defn normalize-idempotency-key
  [value]
  (let [value (some-> value str str/trim)]
    (when (and value (not (str/blank? value)))
      value)))

(defn idempotency-key-from-request
  [req data {:keys [header require? allow-client-ref? max-length]}]
  (let [header-name (str/lower-case (or header "idempotency-key"))
        header-key (normalize-idempotency-key (get-in req [:headers header-name]))
        client-key (when allow-client-ref?
                     (normalize-idempotency-key (:client_ref data)))
        key (or header-key client-key)
        max-length (long (or max-length 0))
        too-long? (and key (pos? max-length) (> (count key) max-length))]
    (cond
      too-long? {:ok false :reason :idempotency-key-too-long}
      (and require? (nil? key)) {:ok false :reason :missing-idempotency-key}
      :else {:ok true
             :key key
             :source (cond
                       header-key :header
                       client-key :client-ref)})))

(defn decode-message
  [payload]
  (cond
    (bytes? payload) (edn/read-string (String. ^bytes payload "UTF-8"))
    (string? payload) (edn/read-string payload)
    :else nil))

(defn min-seq
  [messages]
  (when (seq messages)
    (reduce min (map :seq messages))))

(defn redis-token
  [{:keys [conversation-id cursor direction]}]
  (pagination/encode-token {:conversation_id (str conversation-id)
                            :cursor cursor
                            :direction (name direction)
                            :source "redis"}))

(defn minio-token
  [{:keys [conversation-id cursor direction]}]
  (pagination/encode-token {:conversation_id (str conversation-id)
                            :cursor cursor
                            :direction (name direction)
                            :source "minio"}))

(defn parse-cursor-token
  [cursor-param]
  (let [token (pagination/decode-token cursor-param)
        source (some-> (:source token) keyword)
        direction (some-> (:direction token) keyword)
        cursor (:cursor token)
        seq-cursor (when (number? cursor) (long cursor))
        conv-id (some-> (:conversation_id token) http/parse-uuid)]
    {:token token
     :source source
     :direction direction
     :cursor cursor
     :seq-cursor seq-cursor
     :conversation-id conv-id}))

(defn fetch-minio-page
  [{:keys [db minio segments metrics]} conversation-id cursor limit direction]
  (let [result (segment-reader/fetch-messages {:db db
                                               :minio minio
                                               :segments segments
                                               :metrics metrics}
                                              conversation-id
                                              {:limit limit
                                               :cursor cursor
                                               :direction direction})
        messages (vec (:messages result))
        next-cursor (when (and (seq messages)
                               (:has-more? result))
                      (minio-token {:conversation-id conversation-id
                                    :cursor (:next-seq result)
                                    :direction (or direction :backward)}))]
    {:messages messages
     :next-cursor next-cursor}))

(defn fetch-redis-page
  [redis metrics stream query]
  (let [{:keys [entries next-cursor]} (streams/read! redis metrics stream query)
        messages (->> entries (map :payload) (map decode-message) (remove nil?) vec)]
    {:messages messages
     :next-cursor next-cursor}))

(defn read-minio-history
  [{:keys [db minio segments] :as ctx} conversation-id cursor limit direction]
  (fetch-minio-page ctx conversation-id cursor limit direction))

(defn read-redis-minio-history
  [{:keys [db minio segments metrics]} conversation-id redis stream query token-source token-cursor limit direction]
  (let [redis-cursor (when (= token-source :redis) token-cursor)
        query (cond-> query
                redis-cursor (assoc :cursor redis-cursor))
        {:keys [messages next-cursor]} (fetch-redis-page redis metrics stream query)
        remaining (- limit (count messages))
        before-seq (min-seq messages)
        minio-result (when (and (pos? remaining)
                                (not= direction :forward)
                                minio)
                       (fetch-minio-page {:db db
                                          :minio minio
                                          :segments segments
                                          :metrics metrics}
                                         conversation-id
                                         before-seq
                                         remaining
                                         direction))
        minio-messages (vec (:messages minio-result))
        combined (vec (concat messages minio-messages))
        next-minio (:next-cursor minio-result)
        next-redis (when next-cursor
                     (redis-token {:conversation-id conversation-id
                                   :cursor next-cursor
                                   :direction (or direction :backward)}))
        next-cursor (or next-minio next-redis)]
    {:messages combined
     :next-cursor next-cursor}))

(defn messages-page
  [ctx {:keys [conversation-id redis stream query token-source token-seq token-cursor limit direction]}]
  (if (= token-source :minio)
    (read-minio-history ctx conversation-id token-seq limit direction)
    (read-redis-minio-history ctx conversation-id redis stream query token-source token-cursor limit direction)))

(defn format-messages-response
  [format conversation-id messages next-cursor]
  (http/format-response {:ok true
                         :conversation_id (str conversation-id)
                         :messages messages
                         :next_cursor next-cursor}
                        format))

(defn log-message-create-reject!
  [logger logging {:keys [conversation-id sender-id]} reason details]
  (let [fields (cond-> {:component :messages-create
                        :conversation-id conversation-id
                        :sender-id sender-id
                        :reason reason}
                 (some? details) (assoc :details details))
        debug? (obs-log/log-enabled? logging :debug :messages-create nil)]
    (if debug?
      (obs-log/log! logger logging :debug ::message-create-rejected fields)
      (when logger
        (logger/log logger :info ::message-create-rejected fields)))))

(defn log-message-create!
  [logger logging event fields]
  (let [debug? (obs-log/log-enabled? logging :debug :messages-create nil)]
    (if debug?
      (obs-log/log! logger logging :debug event fields)
      (when logger
        (logger/log logger :info event (dissoc fields :message))))))
