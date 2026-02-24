(ns core-service.app.server.message.authed
  (:require
   [clojure.string :as str]
   [core-service.app.db.attachments :as attachments-db]
   [core-service.app.db.conversations :as conversations-db]
   [core-service.app.libs.redis :as redis-lib]
   [core-service.app.observability.logging :as obs-log]
   [core-service.app.redis.conversation-last :as conversation-last]
   [core-service.app.redis.receipts :as receipts]
   [core-service.app.redis.unread-index :as unread-index]
   [core-service.app.server.message.logic :as logic]
   [d-core.core.stream.protocol :as p-stream]
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

(defn- try-index-message [redis naming conv-id message seq sender-id logger logging log-ctx]
  (try
    (unread-index/index-message! {:redis redis :naming naming}
                                 {:conversation-id conv-id
                                  :message-id (:message_id message)
                                  :seq seq})
    (unread-index/update-last-read-seq! {:redis redis :naming naming}
                                        conv-id sender-id seq)
    (catch Exception e
      (obs-log/log! logger logging :warn ::unread-index-sync-failed
                    (merge log-ctx {:error (.getMessage e)})))))

(defn- try-update-last-message [redis naming metrics message logger logging log-ctx]
  (try
    (conversation-last/update-last-message! {:redis redis
                                             :naming naming
                                             :metrics metrics}
                                            message)
    (catch Exception e
      (obs-log/log! logger logging :warn ::conversation-last-sync-failed
                    (merge log-ctx {:error (.getMessage e)})))))

(defn- try-mark-reference [db attachment-validation logger logging log-ctx]
  (try
    (attachments-db/mark-referenced! db (:attachment-ids @attachment-validation))
    (catch Exception e
      (obs-log/log! logger logging :error ::attachments-mark-referenced-failed
                    (merge log-ctx {:error (.getMessage e)})))))

(defn- create-message! [naming conv-id streams attachment-validation sender-id data idempotency-result logger logging redis metrics db]
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
        payload-bytes (logic/encode-message message)
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
        (try-index-message redis naming conv-id message seq sender-id logger logging log-ctx)
        (try-update-last-message redis naming metrics message logger logging log-ctx)
        (car/wcar (redis-lib/conn redis)
                  (car/publish pubsub-ch payload-bytes))
        (try-mark-reference db attachment-validation logger logging log-ctx)
        (logic/log-message-create! logger logging ::redis-append
                                   (merge log-ctx {:entry-id entry-id}))
        {:ok true
         :conversation_id (str conv-id)
         :message message
         :stream stream
         :entry_id entry-id})
      (catch Exception e
        (obs-log/log! logger logging :error ::redis-append-failed
                      (merge log-ctx {:error (.getMessage e)}))
        (throw e)))))

(defn messages-create
  [{:keys [webdeps]}]
  (let [{:keys [db redis streams naming logger logging idempotency metrics]} webdeps]
    (fn [req]
      (let [data (some-> (get-in req [:parameters :body]) logic/coerce-message-create)
            conv-id (get-in req [:parameters :path :id])
            sender-id (:user-id req)
            idempotency-result (logic/idempotency-key-from-request req data idempotency)
            attachment-validation (delay (validate-message-attachments db conv-id (:attachments data)))]
        (cond
          (nil? sender-id)
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id nil}
                                              :invalid-sender-id nil)
            {:status 401 :body {:ok false :error "invalid sender id"}})

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              :not-a-member nil)
            {:status 403 :body {:ok false :error "not a member"}})

          (not (:ok @attachment-validation))
          (do
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              :invalid-attachment-reference nil)
            {:status 400 :body {:ok false :error (:error @attachment-validation)}})

          (not (:ok idempotency-result))
          (let [reason (:reason idempotency-result)
                error (case reason
                        :missing-idempotency-key "missing idempotency key"
                        :idempotency-key-too-long "invalid idempotency key"
                        "invalid idempotency key")]
            (logic/log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                              reason nil)
            {:status 400 :body {:ok false :error error}})

          :else
          (create-message! naming conv-id streams attachment-validation sender-id data idempotency-result logger logging redis metrics db))))))

(defn messages-list
  [{:keys [webdeps]}]
  (let [{:keys [db streams minio redis naming segments metrics]} webdeps]
    (fn [req]
      (let [conv-id (get-in req [:parameters :path :id])
            sender-id (:user-id req)
            query (get-in req [:parameters :query])
            {:keys [limit cursor direction]} query
            {:keys [token source direction token-direction cursor seq-cursor conversation-id]}
            (logic/parse-cursor-token cursor)
            direction (or (keyword direction)
                          (keyword token-direction)
                          :backward)
            query (assoc query :direction direction)
            token-source source
            token-cursor cursor
            token-seq seq-cursor
            token-conv conversation-id]
        (cond
          (nil? sender-id)
          {:status 401 :body {:ok false :error "invalid sender id"}}

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          {:status 403 :body {:ok false :error "not a member"}}

          (and token-conv (not= token-conv conv-id))
          {:status 400 :body {:ok false :error "cursor conversation mismatch"}}

          (and (= token-source :minio) (not minio))
          {:status 400 :body {:ok false :error "minio not configured"}}

          :else
          (let [stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
                segments (or segments {})
                ctx {:db db :minio minio :segments segments :metrics metrics}
                limit (or limit 50)
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
            {:ok true
             :conversation_id (str conv-id)
             :messages (enrich-message-statuses redis naming metrics conv-id sender-id messages)
             :next_cursor next-cursor}))))))
