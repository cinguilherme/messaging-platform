(ns core-service.app.server.conversation.v1.authed.authed
  (:require [core-service.app.db.conversations :as conversations-db]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.libs.util :as util]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.conversation.v1.authed.logic :as logic]
            [core-service.app.server.http :as http]
            [core-service.app.streams.redis :as streams]
            [core-service.app.observability.logging :as obs-log]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [malli.core :as m]
            [malli.error :as me]
            [taoensso.carmine :as car]))

(defn conversations-create
  [{:keys [webdeps]}]
  (let [{:keys [db logger]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            {:keys [ok data error]} (http/read-json-body req)
            data (when ok (logic/coerce-conversation-create data))
            tenant-id (logic/tenant-id-from-request req)]
        (logger/log logger ::conversations-create-req format)
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
            (http/format-response (assoc result :ok true) format)))))))

(defn conversations-get
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)]
        (cond
          (not conv-id)
          (http/format-response {:ok false :error "invalid conversation id"} format)

          (nil? sender-id)
          (http/format-response {:ok false :error "invalid sender id"} format)

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (http/format-response {:ok false :error "not a member"} format)

          :else
          (let [row (conversations-db/get-conversation db {:conversation-id conv-id})]
            (if-not row
              (http/format-response {:ok false :error "conversation not found"} format)
              (let [members-by-conv (conversations-db/list-memberships db {:conversation-ids [conv-id]})
                    member-ids (get members-by-conv conv-id)
                    profiles (logic/resolve-member-profiles db token-client keycloak member-ids)
                    members (logic/build-member-items member-ids profiles)
                    item (assoc (logic/conversation-row->detail row)
                                :members members)]
                (http/format-response {:ok true :item item} format)))))))))

(def ^:private default-conversation-item-timeout-ms 10000)

(defn conversations-list
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak logger] :as components} webdeps
        item-timeout-ms (or (get webdeps :conversations-list-item-timeout-ms) default-conversation-item-timeout-ms)]
    (fn [req]
      (let [ttap (partial util/ltap logger ::conversations-list-req)
            format (ttap (http/get-accept-format req))
            sender-id (logic/sender-id-from-request req)
            limit (http/parse-long (http/param req "limit") 50)
            cursor-param (http/param req "cursor")
            before-ms (http/parse-long cursor-param ::invalid)
            invalid-cursor? (and (some? cursor-param) (= before-ms ::invalid))
            before-ms (when-not invalid-cursor? before-ms)
            before-ts (logic/timestamp-from-ms before-ms)]
        (cond
          (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
          invalid-cursor? (http/format-response {:ok false :error "invalid cursor"} format)
          :else
          (let [rows (ttap (conversations-db/list-conversations db {:user-id sender-id
                                                                    :limit limit
                                                                    :before-ts before-ts}))
                conv-ids (ttap (mapv :id rows))
                members-by-conv (util/ltap logger ::conversations-list-members-by-conv (conversations-db/list-memberships db {:conversation-ids conv-ids}))
                member-ids (util/ltap logger ::conversations-list-member-ids (->> members-by-conv vals (mapcat identity) distinct ttap))
                profiles (util/ltap logger ::conversations-list-profiles (logic/resolve-member-profiles db token-client keycloak member-ids))
                futures (mapv (fn [row]
                                (let [mids (get members-by-conv (:id row))]
                                  {:row row
                                   :member-ids mids
                                   :future (future (logic/conversation-item components row sender-id mids profiles))}))
                              rows)
                items (util/ltap logger ::conversations-list-items
                                 (mapv (fn [{:keys [row member-ids] f :future}]
                                         (let [result (deref f item-timeout-ms ::timeout)]
                                           (if (= result ::timeout)
                                             (do (when logger (logger/log logger ::conversation-item-timeout {:conv-id (:id row)}))
                                                 (future-cancel f)
                                                 (logic/conversation-item-fallback row sender-id member-ids profiles))
                                             result)))
                                       futures))
                next-cursor (util/ltap logger ::conversations-list-next-cursor (when (= (count rows) limit)
                                                                                 (some-> (last rows) :created_at (.getTime) str)))
                _ (logger/log logger ::conversations-list-end-let items next-cursor)]
            (logger/log logger ::conversations-list-resp items next-cursor)
            (http/format-response {:ok true
                                   :items items
                                   :next_cursor next-cursor}
                                  format)))))))

(defn messages-create
  [{:keys [webdeps]}]
  (let [{:keys [db redis naming metrics logger logging idempotency]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)
            {:keys [ok data error]} (http/read-json-body req)
            data (when ok (logic/coerce-message-create data))
            idempotency-result (logic/idempotency-key-from-request req data idempotency)]
        (cond
          (not conv-id)
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id nil :sender-id sender-id}
                                              :invalid-conversation-id nil)
            (http/format-response {:ok false :error "invalid conversation id"} format))
          (nil? sender-id)
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id nil}
                                              :invalid-sender-id nil)
            (http/format-response {:ok false :error "invalid sender id"} format))
          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              :not-a-member nil)
            (http/format-response {:ok false :error "not a member"} format))
          (not ok)
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              :invalid-json error)
            (http/format-response {:ok false :error error} format))
          (not (m/validate msg-schema/MessageCreateSchema data))
          (let [details (me/humanize (m/explain msg-schema/MessageCreateSchema data))]
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              :invalid-schema details)
            (http/invalid-response format msg-schema/MessageCreateSchema data))
          (not (:ok idempotency-result))
          (let [reason (:reason idempotency-result)
                error (case reason
                        :missing-idempotency-key "missing idempotency key"
                        :idempotency-key-too-long "invalid idempotency key"
                        "invalid idempotency key")]
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              reason nil)
            (http/format-response {:ok false :error error} format))
          :else
          (let [seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
                seq (logic/next-seq! redis metrics seq-key)
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
                         :idempotency-source (:source idempotency-result)
                         :stream stream
                         :payload-bytes (alength payload-bytes)}]
            (logic/log-message-create! logger logging ::message-create
                                       (merge log-ctx {:message message}))
            (try
              (let [entry-id (streams/append! redis metrics stream payload-bytes)
                    pubsub-ch (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)]
                (car/wcar (redis-lib/conn redis)
                          (car/publish pubsub-ch payload-bytes))
                (logic/log-message-create! logger logging ::redis-append
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
                (throw e)))))))))

(defn messages-list
  [{:keys [webdeps]}]
  (let [{:keys [db redis minio naming segments metrics]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)
            limit (http/parse-long (http/param req "limit") 50)
            cursor-param (http/param req "cursor")
            {:keys [token source direction cursor seq-cursor conversation-id]}
            (logic/parse-cursor-token cursor-param)
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
                (logic/messages-page ctx {:conversation-id conv-id
                                          :redis redis
                                          :stream stream
                                          :query query
                                          :token-source token-source
                                          :token-seq token-seq
                                          :token-cursor token-cursor
                                          :limit limit
                                          :direction direction})]
            (logic/format-messages-response format conv-id messages next-cursor)))))))

(defn receipts-create
  [{:keys [webdeps]}]
  (let [{:keys [db redis naming receipt metrics]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)
            {:keys [ok data error]} (http/read-json-body req)
            data (when ok (logic/coerce-receipt-create data))]
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
                                  format)))))))

(defmethod ig/init-key :core-service.app.server.conversation.v1.authed/routes
  [_ {:keys [webdeps]}]
  ["/v1/conversations"
   ["" {:post (conversations-create {:webdeps webdeps})
        :get (conversations-list {:webdeps webdeps})}]
   ["/:id" {:get (conversations-get {:webdeps webdeps})}]
   ["/:id/messages" {:post (messages-create {:webdeps webdeps})
                     :get (messages-list {:webdeps webdeps})}]
   ["/:id/receipts" {:post (receipts-create {:webdeps webdeps})}]])
