(ns core-service.app.server.conversation.v1.authed
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.db.users :as users-db]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.pagination :as pagination]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.server.http :as http]
            [core-service.app.streams.redis :as streams]
            [core-service.app.observability.logging :as obs-log]
            [d-core.core.auth.token-client :as token-client]
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

(defn- timestamp-from-ms
  [value]
  (when (some? value)
    (try
      (java.sql.Timestamp. (long value))
      (catch Exception _
        nil))))

(defn- sender-id-from-request
  [req]
  (or (http/parse-uuid (get-in req [:auth/principal :subject]))
      (http/parse-uuid (get-in req [:auth/principal :user_id]))))

(defn- coerce-message-create
  [data]
  (-> data
      (update :type (fn [v] (if (string? v) (keyword v) v)))))

(defn- normalize-idempotency-key
  [value]
  (let [value (some-> value str str/trim)]
    (when (and value (not (str/blank? value)))
      value)))

(defn- idempotency-key-from-request
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

(defn- conversation-row->item
  [row]
  (let [created-at (:created_at row)
        updated-at (when created-at (.getTime ^java.util.Date created-at))
        type-val (some-> (:type row) keyword)]
    {:conversation_id (str (:id row))
     :type (or type-val (:type row))
     :title (:title row)
     :updated_at updated-at}))

(defn- attribute-value
  [attrs k]
  (let [v (or (get attrs k) (get attrs (keyword (name k))))]
    (cond
      (vector? v) (first v)
      (sequential? v) (first v)
      :else v)))

(defn- keycloak-user->profile
  [user]
  (let [attrs (:attributes user)]
    {:username (:username user)
     :first_name (:firstName user)
     :last_name (:lastName user)
     :avatar_url (or (attribute-value attrs :avatar_url)
                     (attribute-value attrs :avatarUrl))
     :email (:email user)
     :enabled (:enabled user)}))

(defn- profiles-by-id
  [rows]
  (reduce (fn [acc row]
            (let [user-id (some-> (:user_id row) str)]
              (if user-id
                (assoc acc user-id (assoc row :user_id user-id))
                acc)))
          {}
          rows))

(defn- fetch-keycloak-profiles
  [{:keys [token-client keycloak]} user-ids]
  (let [user-ids (->> user-ids (map str) distinct vec)]
    (if (or (empty? user-ids)
            (nil? token-client)
            (nil? (:admin-url keycloak)))
      {}
      (try
        (let [admin-token (token-client/client-credentials token-client {})
              access-token (:access-token admin-token)
              admin-url (:admin-url keycloak)
              http-opts (:http-opts keycloak)
              headers {"authorization" (str "Bearer " access-token)}]
          (reduce (fn [acc user-id]
                    (let [resp (http-client/get (str admin-url "/users/" user-id)
                                                (merge {:headers headers
                                                        :as :text
                                                        :throw-exceptions false}
                                                       http-opts))
                          status (:status resp)
                          parsed (some-> (:body resp) (json/parse-string true))]
                      (if (<= 200 status 299)
                        (assoc acc user-id (keycloak-user->profile parsed))
                        acc)))
                  {}
                  user-ids))
        (catch Exception _
          {})))))

(defn- build-member-items
  [member-ids profiles]
  (mapv (fn [user-id]
          (let [user-id-str (str user-id)
                profile (get profiles user-id-str)]
            (merge {:user_id user-id-str} profile)))
        (or member-ids [])))

(defn- direct-conversation?
  [conversation-type]
  (or (= conversation-type :direct)
      (= conversation-type "direct")))

(defn- build-counterpart
  [member-ids profiles sender-id]
  (let [member-ids (or member-ids [])
        other-id (first (remove #(= % sender-id) member-ids))]
    (when other-id
      (let [user-id-str (str other-id)
            profile (get profiles user-id-str)]
        (merge {:user_id user-id-str} profile)))))

(defn- redis-last-message
  [redis metrics stream]
  (when (and redis stream)
    (let [{:keys [entries]} (streams/read! redis metrics stream {:direction :backward :limit 1})
          payload (:payload (first entries))]
      (when payload
        (decode-message payload)))))

(defn- minio-last-message
  [{:keys [db minio segments]} conversation-id]
  (when minio
    (let [segments (or segments {})
          result (segment-reader/fetch-messages {:db db
                                                 :minio minio
                                                 :segments segments}
                                                conversation-id
                                                {:limit 1
                                                 :cursor nil
                                                 :direction :backward})
          messages (vec (:messages result))]
      (first messages))))

(defn- receipt-read?
  [redis metrics naming conversation-id message-id user-id]
  (let [key (receipts/receipt-key naming conversation-id message-id)
        field (str "read:" user-id)]
    (app-metrics/with-redis metrics :hget
      #(car/wcar (redis-lib/conn redis)
         (car/hget key field)))))

(defn- unread-count-from-redis
  [redis metrics naming conversation-id user-id]
  (when (and redis naming conversation-id user-id)
    (let [stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conversation-id)
          batch-size 100]
      (loop [cursor nil
             unread 0]
        (let [{:keys [entries next-cursor]} (streams/read! redis metrics stream {:direction :backward
                                                                                :limit batch-size
                                                                                :cursor cursor})
              remaining (seq entries)]
          (if-not remaining
            unread
            (loop [entries remaining
                   unread unread]
              (if-not (seq entries)
                (if next-cursor
                  (recur next-cursor unread)
                  unread)
                (let [payload (:payload (first entries))
                      message (when payload (decode-message payload))
                      sender-id (:sender_id message)
                      message-id (:message_id message)
                      readable? (and message-id sender-id (not= sender-id user-id))
                      read? (and readable?
                                 (receipt-read? redis metrics naming conversation-id message-id user-id))]
                  (cond
                    (and readable? read?) unread
                    readable? (recur (rest entries) (inc unread))
                    :else (recur (rest entries) unread)))))))))))

(defn- conversation-item
  [ctx row user-id member-ids profiles]
  (let [conv-id (:id row)
        naming (:naming ctx)
        redis (:redis ctx)
        metrics (:metrics ctx)
        stream (when (and naming conv-id)
                 (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id))
        last-message (or (redis-last-message redis metrics stream)
                         (minio-last-message ctx conv-id))
        unread-count (or (unread-count-from-redis redis metrics naming conv-id user-id) 0)
        base (conversation-row->item row)
        members (build-member-items member-ids profiles)
        counterpart (when (direct-conversation? (:type base))
                      (build-counterpart member-ids profiles user-id))]
    (cond-> (assoc base
                   :members members
                   :last_message last-message
                   :unread_count unread-count)
      counterpart (assoc :counterpart counterpart))))

(defn conversations-list
  [{:keys [db redis naming metrics minio segments token-client keycloak]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          sender-id (sender-id-from-request req)
          limit (http/parse-long (http/param req "limit") 50)
          cursor-param (http/param req "cursor")
          before-ms (http/parse-long cursor-param ::invalid)
          invalid-cursor? (and (some? cursor-param) (= before-ms ::invalid))
          before-ms (when-not invalid-cursor? before-ms)
          before-ts (timestamp-from-ms before-ms)
          ctx {:db db
               :redis redis
               :naming naming
               :metrics metrics
               :minio minio
               :segments segments}]
      (cond
        (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
        invalid-cursor? (http/format-response {:ok false :error "invalid cursor"} format)
        :else
        (let [rows (conversations-db/list-conversations db {:user-id sender-id
                                                            :limit limit
                                                            :before-ts before-ts})
              conv-ids (mapv :id rows)
              members-by-conv (conversations-db/list-memberships db {:conversation-ids conv-ids})
              member-ids (->> members-by-conv vals (mapcat identity) distinct)
              local-profiles (users-db/fetch-user-profiles db {:user-ids member-ids})
              profiles-by-id (profiles-by-id local-profiles)
              missing-ids (remove #(contains? profiles-by-id (str %)) member-ids)
              fallback-profiles (fetch-keycloak-profiles {:token-client token-client
                                                          :keycloak keycloak}
                                                         missing-ids)
              _ (doseq [[user-id profile] fallback-profiles]
                  (users-db/upsert-user-profile! db (assoc profile :user-id user-id)))
              profiles (merge profiles-by-id fallback-profiles)
              items (mapv (fn [row]
                            (let [member-ids (get members-by-conv (:id row))]
                              (conversation-item ctx row sender-id member-ids profiles)))
                          rows)
              next-cursor (when (= (count rows) limit)
                            (some-> (last rows) :created_at (.getTime) str))]
          (http/format-response {:ok true
                                 :items items
                                 :next_cursor next-cursor}
                                format))))))

(defn messages-create
  [{:keys [db redis naming metrics logger logging idempotency]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          sender-id (sender-id-from-request req)
          {:keys [ok data error]} (http/read-json-body req)
          data (when ok (coerce-message-create data))
          idempotency-result (idempotency-key-from-request req data idempotency)]
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
        (not (:ok idempotency-result))
        (let [reason (:reason idempotency-result)
              error (case reason
                      :missing-idempotency-key "missing idempotency key"
                      :idempotency-key-too-long "invalid idempotency key"
                      "invalid idempotency key")]
          (log-message-create-reject! logger logging {:conversation-id conv-id :sender-id sender-id}
                                      reason nil)
          (http/format-response {:ok false :error error} format))
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
                       :idempotency-source (:source idempotency-result)
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
