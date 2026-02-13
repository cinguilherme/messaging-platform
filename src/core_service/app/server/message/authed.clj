(ns core-service.app.server.message.authed
  (:require [clojure.string :as str]
            [core-service.app.db.attachments :as attachments-db]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.message.logic :as logic]
            [core-service.app.server.http :as http]
            [d-core.core.stream.protocol :as p-stream]
            [malli.core :as m]
            [malli.error :as me]
            [taoensso.carmine :as car]))

(defn- attachment-row-expired?
  [row now-ms]
  (let [expires-at (:expires_at row)]
    (and expires-at
         (<= (.getTime ^java.util.Date expires-at) now-ms))))

(def ^:private trusted-attachment-fields
  [:attachment_id :object_key :mime_type :size_bytes :checksum])

(defn- canonical-attachment
  [row]
  (select-keys row trusted-attachment-fields))

(defn- same-attachment?
  [payload-att row]
  (and (= (:attachment_id payload-att) (:attachment_id row))
       (= (:object_key payload-att) (:object_key row))
       (= (:mime_type payload-att) (:mime_type row))))

(defn- validate-message-attachments
  [db conv-id attachments]
  (let [attachments (vec (or attachments []))]
    (if-not (seq attachments)
      {:ok true :attachment-ids [] :canonical-attachments []}
      (let [ids (mapv :attachment_id attachments)]
        (if-not (every? uuid? ids)
          {:ok false :error "invalid attachment reference"}
          (let [rows (attachments-db/fetch-attachments-by-ids db ids)
                rows-by-id (reduce (fn [acc row] (assoc acc (:attachment_id row) row)) {} rows)
                now-ms (System/currentTimeMillis)
                valid? (every? (fn [payload-att]
                                 (let [row (get rows-by-id (:attachment_id payload-att))]
                                   (and row
                                        (= conv-id (:conversation_id row))
                                        (not (attachment-row-expired? row now-ms))
                                        (same-attachment? payload-att row))))
                               attachments)]
            (if valid?
              {:ok true
               :attachment-ids (vec (distinct ids))
               :canonical-attachments (mapv (fn [payload-att]
                                              (canonical-attachment (get rows-by-id (:attachment_id payload-att))))
                                            attachments)}
              {:ok false :error "invalid attachment reference"})))))))

(defn- has-foreign-receipt?
  [entries prefix sender-id]
  (let [prefix-len (count prefix)
        sender (str sender-id)]
    (boolean
     (some (fn [field]
             (and (string? field)
                  (str/starts-with? field prefix)
                  (not= (subs field prefix-len) sender)))
           (keys entries)))))

(defn- enrich-message-statuses
  [redis naming metrics conversation-id requester-id messages]
  (let [own-message-ids (->> messages
                             (filter #(= requester-id (:sender_id %)))
                             (map :message_id)
                             (remove nil?)
                             vec)
        receipts-by-message-id (receipts/batch-get-receipts
                                {:redis redis
                                 :naming naming
                                 :metrics metrics}
                                {:conversation-id conversation-id
                                 :message-ids own-message-ids})]
    (mapv (fn [message]
            (if (not= requester-id (:sender_id message))
              message
              (let [entries (get receipts-by-message-id (:message_id message) {})
                    status (cond
                             (has-foreign-receipt? entries "read:" requester-id) "read"
                             (has-foreign-receipt? entries "delivered:" requester-id) "delivered"
                             :else "sent")]
                (assoc message :status status))))
          messages)))

(defn messages-create
  [{:keys [webdeps]}]
  (let [{:keys [db redis streams naming logger logging idempotency]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)
            {:keys [ok data error]} (http/read-json-body req)
            data (when ok (logic/coerce-message-create data))
            idempotency-result (logic/idempotency-key-from-request req data idempotency)
            attachment-validation (delay (validate-message-attachments db conv-id (:attachments data)))]
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
          (not (:ok @attachment-validation))
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              :invalid-attachment-reference nil)
            (http/format-response {:ok false
                                   :error (:error @attachment-validation)}
                                  format))
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
                seq (logic/next-seq! streams seq-key)
                canonical-attachments (:canonical-attachments @attachment-validation)
                message {:message_id (java.util.UUID/randomUUID)
                         :conversation_id conv-id
                         :seq seq
                         :sender_id sender-id
                         :sent_at (System/currentTimeMillis)
                         :type (:type data)
                         :body (:body data)
                         :attachments canonical-attachments
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
              (let [entry-id (p-stream/append-payload! streams stream payload-bytes)
                    pubsub-ch (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)]
                (car/wcar (redis-lib/conn redis)
                          (car/publish pubsub-ch payload-bytes))
                (try
                  (attachments-db/mark-referenced! db (:attachment-ids @attachment-validation))
                  (catch Exception e
                    (obs-log/log! logger logging :error ::attachments-mark-referenced-failed
                                  (merge log-ctx {:error (.getMessage e)}))))
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
  (let [{:keys [db streams minio redis naming segments metrics]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)
            limit (http/parse-long (http/param req "limit") 50)
            cursor-param (http/param req "cursor")
            {:keys [token source direction cursor seq-cursor conversation-id]}
            (logic/parse-cursor-token cursor-param)
            direction (or (some-> (http/param req "direction") keyword)
                          direction
                          :backward)
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
                                          :streams streams
                                          :stream stream
                                          :query query
                                          :token-source token-source
                                          :token-seq token-seq
                                          :token-cursor token-cursor
                                          :limit limit
                                          :direction direction})]
            (logic/format-messages-response format
                                           conv-id
                                           (enrich-message-statuses redis naming metrics conv-id sender-id messages)
                                           next-cursor)))))))
