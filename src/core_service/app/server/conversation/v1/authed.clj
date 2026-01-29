(ns core-service.app.server.conversation.v1.authed
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.pagination :as pagination]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.server.http :as http]
            [core-service.app.streams.redis :as streams]
            [core-service.app.observability.logging :as obs-log]
            [duct.logger :as logger]
            [malli.core :as m]
            [malli.error :as me]
            [taoensso.carmine :as car]))

(defn- next-seq!
  [redis-client metrics key]
  (app-metrics/with-redis metrics :incr
    #(car/wcar (redis-lib/conn redis-client)
       (car/incr key))))

(defn- coerce-conversation-create
  [data]
  (-> data
      (update :type (fn [v] (if (string? v) (keyword v) v)))
      (update :title (fn [v] (when (and (string? v) (not (str/blank? v))) v)))
      (update :member_ids (fn [ids]
                            (when (sequential? ids)
                              (->> ids
                                   (map http/parse-uuid)
                                   vec))))))

(defn- tenant-id-from-request
  [req]
  (or (get-in req [:auth/principal :tenant-id])
      (get-in req [:auth/principal :tenant_id])))

(defn- sender-id-from-request
  [req]
  (or (http/parse-uuid (get-in req [:auth/principal :subject]))
      (http/parse-uuid (get-in req [:auth/principal :user_id]))))

(defn- coerce-message-create
  [data]
  (-> data
      (update :type (fn [v] (if (string? v) (keyword v) v)))))

(defn- coerce-receipt-create
  [data]
  (cond-> data
    (contains? data :receipt_type)
    (update :receipt_type (fn [v] (if (string? v) (keyword v) v)))
    (contains? data :message_id)
    (update :message_id http/parse-uuid)
    (contains? data :at)
    (update :at (fn [v] (when (some? v) (http/parse-long v nil))))))

(defn- decode-message
  [payload]
  (cond
    (bytes? payload) (edn/read-string (String. ^bytes payload "UTF-8"))
    (string? payload) (edn/read-string payload)
    :else nil))

(defn- min-seq
  [messages]
  (when (seq messages)
    (reduce min (map :seq messages))))

(defn- redis-token
  [{:keys [conversation-id cursor direction]}]
  (pagination/encode-token {:conversation_id (str conversation-id)
                            :cursor cursor
                            :direction (name direction)
                            :source "redis"}))

(defn- minio-token
  [{:keys [conversation-id cursor direction]}]
  (pagination/encode-token {:conversation_id (str conversation-id)
                            :cursor cursor
                            :direction (name direction)
                            :source "minio"}))

(defn- parse-cursor-token
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

(defn- fetch-minio-page
  [{:keys [db minio segments]} conversation-id cursor limit direction]
  (let [result (segment-reader/fetch-messages {:db db
                                               :minio minio
                                               :segments segments}
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

(defn- fetch-redis-page
  [redis metrics stream query]
  (let [{:keys [entries next-cursor]} (streams/read! redis metrics stream query)
        messages (->> entries (map :payload) (map decode-message) (remove nil?) vec)]
    {:messages messages
     :next-cursor next-cursor}))

(defn- read-minio-history
  [{:keys [db minio segments] :as ctx} conversation-id cursor limit direction]
  (fetch-minio-page ctx conversation-id cursor limit direction))

(defn- read-redis-minio-history
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
                                          :segments segments}
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

(defn- messages-page
  [ctx {:keys [conversation-id redis stream query token-source token-seq token-cursor limit direction]}]
  (if (= token-source :minio)
    (read-minio-history ctx conversation-id token-seq limit direction)
    (read-redis-minio-history ctx conversation-id redis stream query token-source token-cursor limit direction)))

(defn- format-messages-response
  [format conversation-id messages next-cursor]
  (http/format-response {:ok true
                         :conversation_id (str conversation-id)
                         :messages messages
                         :next_cursor next-cursor}
                        format))

(defn- log-message-create-reject!
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

(defn- log-message-create!
  [logger logging event fields]
  (let [debug? (obs-log/log-enabled? logging :debug :messages-create nil)]
    (if debug?
      (obs-log/log! logger logging :debug event fields)
      (when logger
        (logger/log logger :info event (dissoc fields :message))))))

(defn conversations-create
  [{:keys [db]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)
          data (when ok (coerce-conversation-create data))
          tenant-id (tenant-id-from-request req)]
      (cond
        (not ok) (http/format-response {:ok false :error error} format)
        (nil? tenant-id) (http/format-response {:ok false :error "missing tenant"} format)
        (not (m/validate msg-schema/ConversationCreateSchema data))
        (http/invalid-response format msg-schema/ConversationCreateSchema data)
        (empty? (:member_ids data))
        (http/format-response {:ok false :error "member_ids cannot be empty"} format)
        :else
        (let [result (conversations-db/create-conversation!
                       db
                       {:tenant-id tenant-id
                        :type (:type data)
                        :title (:title data)
                        :member-ids (:member_ids data)})]
          (http/format-response (assoc result :ok true) format))))))

(defn conversations-get
  [_options]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))]
      (if-not conv-id
        (http/format-response {:ok false :error "invalid conversation id"} format)
        (http/format-response {:ok true :conversation_id (str conv-id)} format)))))

(defn messages-create
  [{:keys [db redis naming metrics logger logging]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          {:keys [ok data error]} (http/read-json-body req)
          data (when ok (coerce-message-create data))]
      (cond
        (not conv-id)
        (do
          (log-message-create-reject! logger logging {:conversation-id nil :sender-id sender-id}
                                      :invalid-conversation-id nil)
          (http/format-response {:ok false :error "invalid conversation id"} format))
        (nil? sender-id)
        (do
          (log-message-create-reject! logger logging {:conversation-id conv-id :sender-id nil}
                                      :invalid-sender-id nil)
          (http/format-response {:ok false :error "invalid sender id"} format))
        (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
        (do
          (log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                      :not-a-member nil)
          (http/format-response {:ok false :error "not a member"} format))
        (not ok)
        (do
          (log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                      :invalid-json error)
          (http/format-response {:ok false :error error} format))
        (not (m/validate msg-schema/MessageCreateSchema data))
        (let [details (me/humanize (m/explain msg-schema/MessageCreateSchema data))]
          (log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                      :invalid-schema details)
          (http/invalid-response format msg-schema/MessageCreateSchema data))
        :else
        (let [seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
              seq (next-seq! redis metrics seq-key)
              message {:message_id (java.util.UUID/randomUUID)
                       :conversation_id conv-id
                       :seq seq
                       :sender_id sender-id
                       :sent_at (System/currentTimeMillis)
                       :type (:type data)
                       :body (:body data)
                       :attachments (or (:attachments data) [])
                       :client_ref (:client_ref data)
                       :meta (:meta data)}
              stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
              payload-bytes (.getBytes (pr-str message) "UTF-8")
              log-ctx {:component :messages-create
                       :conversation-id conv-id
                       :sender-id sender-id
                       :seq seq
                       :stream stream
                       :payload-bytes (alength payload-bytes)}]
          (log-message-create! logger logging ::message-create
                               (merge log-ctx {:message message}))
          (try
            (let [entry-id (streams/append! redis metrics stream payload-bytes)]
              (log-message-create! logger logging ::redis-append
                                   (merge log-ctx {:entry-id entry-id}))
              (http/format-response {:ok true
                                     :conversation_id (str conv-id)
                                     :message message
                                     :stream stream
                                     :entry_id entry-id}
                                    format))
            (catch Exception e
              (obs-log/log! logger logging :error ::redis-append-failed
                            (merge log-ctx {:error (.getMessage e)}))
              (throw e))))))))

(defn messages-list
  [{:keys [db redis minio naming segments metrics]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          limit (http/parse-long (http/param req "limit") 50)
          cursor-param (http/param req "cursor")
          {:keys [token source direction cursor seq-cursor conversation-id]}
          (parse-cursor-token cursor-param)
          direction (or (some-> (http/param req "direction") keyword)
                        direction)
          query (cond-> {:limit limit}
                  (and cursor-param (not token)) (assoc :cursor cursor-param)
                  direction (assoc :direction direction))
          token-source source
          token-cursor cursor
          token-seq seq-cursor
          token-conv conversation-id]
      (cond
        (not conv-id) (http/format-response {:ok false :error "invalid conversation id"} format)
        (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
        (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
        (http/format-response {:ok false :error "not a member"} format)
        (not (m/validate msg-schema/PaginationQuerySchema query))
        (http/format-response {:ok false
                               :error "invalid query"
                               :details (me/humanize (m/explain msg-schema/PaginationQuerySchema query))}
                              format)
        (and token-conv (not= token-conv conv-id))
        (http/format-response {:ok false :error "cursor conversation mismatch"} format)
        (and (= token-source :minio) (not minio))
        (http/format-response {:ok false :error "minio not configured"} format)
        :else
        (let [stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
              segments (or segments {})
              ctx {:db db :minio minio :segments segments :metrics metrics}
              {:keys [messages next-cursor]}
              (messages-page ctx {:conversation-id conv-id
                                  :redis redis
                                  :stream stream
                                  :query query
                                  :token-source token-source
                                  :token-seq token-seq
                                  :token-cursor token-cursor
                                  :limit limit
                                  :direction direction})]
          (format-messages-response format conv-id messages next-cursor))))))

(defn receipts-create
  [{:keys [db redis naming receipt metrics]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          {:keys [ok data error]} (http/read-json-body req)
          data (when ok (coerce-receipt-create data))]
      (cond
        (not conv-id) (http/format-response {:ok false :error "invalid conversation id"} format)
        (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
        (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
        (http/format-response {:ok false :error "not a member"} format)
        (not ok) (http/format-response {:ok false :error error} format)
        (not (m/validate msg-schema/ReceiptCreateSchema data))
        (http/invalid-response format msg-schema/ReceiptCreateSchema data)
        :else
        (do
          (receipts/record! {:redis redis
                             :naming naming
                             :receipt receipt
                             :metrics metrics}
                            {:conversation-id conv-id
                             :message-id (:message_id data)
                             :user-id sender-id
                             :receipt-type (:receipt_type data)
                             :at (:at data)})
          (http/format-response {:ok true
                                 :conversation_id (str conv-id)
                                 :message_id (str (:message_id data))
                                 :receipt_type (name (:receipt_type data))}
                                format))))))
