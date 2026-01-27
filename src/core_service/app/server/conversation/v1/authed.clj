(ns core-service.app.server.conversation.v1.authed
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.pagination :as pagination]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.server.http :as http]
            [core-service.app.streams.redis :as streams]
            [malli.core :as m]
            [malli.error :as me]
            [taoensso.carmine :as car]))

(defn- redis-conn
  [redis-client]
  (:conn redis-client))

(defn- next-seq!
  [redis-client key]
  (car/wcar (redis-conn redis-client)
    (car/incr key)))

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
  [{:keys [db redis naming]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          {:keys [ok data error]} (http/read-json-body req)
          data (when ok (coerce-message-create data))]
      (cond
        (not conv-id) (http/format-response {:ok false :error "invalid conversation id"} format)
        (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
        (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
        (http/format-response {:ok false :error "not a member"} format)
        (not ok) (http/format-response {:ok false :error error} format)
        (not (m/validate msg-schema/MessageCreateSchema data))
        (http/invalid-response format msg-schema/MessageCreateSchema data)
        :else
        (let [seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
              seq (next-seq! redis seq-key)
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
              entry-id (streams/append! redis stream payload-bytes)]
          (http/format-response {:ok true
                                 :conversation_id (str conv-id)
                                 :message message
                                 :stream stream
                                 :entry_id entry-id}
                                format))))))

(defn messages-list
  [{:keys [db redis minio naming segments]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          limit (http/parse-long (http/param req "limit") 50)
          cursor-param (http/param req "cursor")
          token (pagination/decode-token cursor-param)
          token-source (some-> (:source token) keyword)
          token-direction (some-> (:direction token) keyword)
          token-cursor (:cursor token)
          token-seq (when (number? token-cursor) (long token-cursor))
          token-conv (some-> (:conversation_id token) http/parse-uuid)
          direction (or (some-> (http/param req "direction") keyword)
                        token-direction)
          query (cond-> {:limit limit}
                  (and cursor-param (not token)) (assoc :cursor cursor-param)
                  direction (assoc :direction direction))]
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
              read-from-minio (fn [cursor remaining direction]
                                (when (and minio (pos? remaining))
                                  (segment-reader/fetch-messages {:db db
                                                                  :minio minio
                                                                  :segments segments}
                                                                 conv-id
                                                                 {:limit remaining
                                                                  :cursor cursor
                                                                  :direction direction})))]
          (if (= token-source :minio)
            (let [minio-result (read-from-minio token-seq limit direction)
                  minio-messages (vec (:messages minio-result))
                  next-cursor (when (and (seq minio-messages)
                                         (:has-more? minio-result))
                                (minio-token {:conversation-id conv-id
                                              :cursor (:next-seq minio-result)
                                              :direction (or direction :backward)}))]
              (http/format-response {:ok true
                                     :conversation_id (str conv-id)
                                     :messages minio-messages
                                     :next_cursor next-cursor}
                                    format))
            (let [redis-cursor (when (= token-source :redis) token-cursor)
                  query (cond-> query
                          redis-cursor (assoc :cursor redis-cursor))
                  {:keys [entries next-cursor]} (streams/read! redis stream query)
                  redis-messages (->> entries (map :payload) (map decode-message) (remove nil?) vec)
                  remaining (- limit (count redis-messages))
                  before-seq (min-seq redis-messages)
                  minio-result (when (and (pos? remaining)
                                          (not= direction :forward))
                                 (read-from-minio before-seq remaining direction))
                  minio-messages (vec (:messages minio-result))
                  combined (vec (concat redis-messages minio-messages))
                  next-minio (when (and (seq minio-messages)
                                        (:has-more? minio-result))
                               (minio-token {:conversation-id conv-id
                                             :cursor (:next-seq minio-result)
                                             :direction (or direction :backward)}))
                  next-redis (when next-cursor
                               (redis-token {:conversation-id conv-id
                                             :cursor next-cursor
                                             :direction (or direction :backward)}))
                  next-cursor (or next-minio next-redis)]
              (http/format-response {:ok true
                                     :conversation_id (str conv-id)
                                     :messages combined
                                     :next_cursor next-cursor}
                                    format))))))))
