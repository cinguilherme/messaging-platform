(ns core-service.app.server.conversation.v1.authed
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.http :as http]
            [d-core.core.producers.protocol :as producer]
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

(defn- publish!
  [redis-client channel payload]
  (car/wcar (redis-conn redis-client)
    (car/publish channel payload)))

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
  [{:keys [db redis producer naming]}]
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
              channel (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)
              ack (producer/produce! producer message {:topic :messages
                                                       :stream stream
                                                       :producer :redis})]
          (publish! redis channel (json/generate-string message))
          (http/format-response {:ok true
                                 :conversation_id (str conv-id)
                                 :message message
                                 :stream stream
                                 :publish ack}
                                format))))))

(defn messages-list
  [{:keys [db]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          limit (http/parse-long (http/param req "limit") 50)
          cursor (http/param req "cursor")
          direction (some-> (http/param req "direction") keyword)
          query {:limit limit
                 :cursor cursor
                 :direction direction}]
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
        :else
        (http/format-response {:ok true
                               :conversation_id (str conv-id)
                               :messages []
                               :next_cursor nil}
                              format)))))
