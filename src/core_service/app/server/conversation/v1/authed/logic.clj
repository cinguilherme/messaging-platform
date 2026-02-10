(ns core-service.app.server.conversation.v1.authed.logic
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.db.users :as users-db]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.pagination :as pagination]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.server.http :as http]
            [core-service.app.streams.redis :as streams]
            [d-core.core.auth.token-client :as token-client]
            [duct.logger :as logger]
            [taoensso.carmine :as car]))

(declare resolve-member-profiles build-member-items conversation-row->detail)

(defn next-seq!
  [redis-client metrics key]
  (app-metrics/with-redis metrics :incr
    #(car/wcar (redis-lib/conn redis-client)
               (car/incr key))))

(defn coerce-conversation-create
  [data]
  (-> data
      (update :type (fn [v] (if (string? v) (keyword v) v)))
      (update :title (fn [v] (when (and (string? v) (not (str/blank? v))) v)))
      (update :member_ids (fn [ids]
                            (when (sequential? ids)
                              (->> ids
                                   (map http/parse-uuid)
                                   vec))))))

(defn tenant-id-from-request
  [req]
  (or (get-in req [:auth/principal :tenant-id])
      (get-in req [:auth/principal :tenant_id])))

(defn timestamp-from-ms
  [value]
  (when (some? value)
    (try
      (java.sql.Timestamp. (long value))
      (catch Exception _
        nil))))

(defn sender-id-from-request
  [req]
  (or (http/parse-uuid (get-in req [:auth/principal :subject]))
      (http/parse-uuid (get-in req [:auth/principal :user_id]))))

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

(defn coerce-receipt-create
  [data]
  (cond-> data
    (contains? data :receipt_type)
    (update :receipt_type (fn [v] (if (string? v) (keyword v) v)))
    (contains? data :message_id)
    (update :message_id http/parse-uuid)
    (contains? data :at)
    (update :at (fn [v] (when (some? v) (http/parse-long v nil))))))

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


(defn conversation-row->item
  [row]
  (let [created-at (:created_at row)
        updated-at (when created-at (.getTime ^java.util.Date created-at))
        type-val (some-> (:type row) keyword)]
    {:conversation_id (str (:id row))
     :type (or type-val (:type row))
     :title (:title row)
     :updated_at updated-at}))

(defn conversation-row->detail
  [row]
  (let [created-at (:created_at row)
        created-ms (when created-at (.getTime ^java.util.Date created-at))
        type-val (some-> (:type row) keyword)]
    {:conversation_id (str (:id row))
     :type (or type-val (:type row))
     :title (:title row)
     :created_at created-ms
     :updated_at created-ms}))

(defn attribute-value
  [attrs k]
  (let [v (or (get attrs k) (get attrs (keyword (name k))))]
    (cond
      (vector? v) (first v)
      (sequential? v) (first v)
      :else v)))

(defn keycloak-user->profile
  [user]
  (let [attrs (:attributes user)]
    {:username (:username user)
     :first_name (:firstName user)
     :last_name (:lastName user)
     :avatar_url (or (attribute-value attrs :avatar_url)
                     (attribute-value attrs :avatarUrl))
     :email (:email user)
     :enabled (:enabled user)}))

(defn profiles-by-id
  [rows]
  (reduce (fn [acc row]
            (let [user-id (some-> (:user_id row) str)]
              (if user-id
                (assoc acc user-id (assoc row :user_id user-id))
                acc)))
          {}
          rows))

(defn fetch-keycloak-profiles
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

(defn resolve-member-profiles
  [db token-client keycloak member-ids]
  (let [local-profiles (users-db/fetch-user-profiles db {:user-ids member-ids})
        profiles (profiles-by-id local-profiles)
        missing-ids (remove #(contains? profiles (str %)) member-ids)
        fallback-profiles (fetch-keycloak-profiles {:token-client token-client
                                                    :keycloak keycloak}
                                                   missing-ids)]
    (doseq [[user-id profile] fallback-profiles]
      (users-db/upsert-user-profile! db (assoc profile :user-id user-id)))
    (merge profiles fallback-profiles)))

(defn build-member-items
  [member-ids profiles]
  (mapv (fn [user-id]
          (let [user-id-str (str user-id)
                profile (get profiles user-id-str)]
            (merge {:user_id user-id-str} profile)))
        (or member-ids [])))

(defn direct-conversation?
  [conversation-type]
  (or (= conversation-type :direct)
      (= conversation-type "direct")))

(defn build-counterpart
  [member-ids profiles sender-id]
  (let [member-ids (or member-ids [])
        other-id (first (remove #(= % sender-id) member-ids))]
    (when other-id
      (let [user-id-str (str other-id)
            profile (get profiles user-id-str)]
        (merge {:user_id user-id-str} profile)))))

(defn redis-last-message
  [redis metrics stream]
  (when (and redis stream)
    (let [{:keys [entries]} (streams/read! redis metrics stream {:direction :backward :limit 1})
          payload (:payload (first entries))]
      (when payload
        (decode-message payload)))))

(defn minio-last-message
  [{:keys [db minio segments] :as components} conversation-id]
  (when minio
    (let [segments (or segments {})
          result (segment-reader/fetch-messages components conversation-id {:limit 1 :cursor nil :direction :backward})
          messages (vec (:messages result))]
      (first messages))))

(defn receipt-read?
  [redis metrics naming conversation-id message-id user-id]
  (let [key (receipts/receipt-key naming conversation-id message-id)
        field (str "read:" user-id)]
    (app-metrics/with-redis metrics :hget
      #(car/wcar (redis-lib/conn redis)
                 (car/hget key field)))))

(defn batch-receipt-read?
  "Check read receipts for multiple messages in a single Redis pipeline.
  Returns a map of message-id -> receipt-value (nil when not read)."
  [redis metrics naming conversation-id message-ids user-id]
  (if (empty? message-ids)
    {}
    (let [keys-and-fields (mapv (fn [mid]
                                  [(receipts/receipt-key naming conversation-id mid)
                                   (str "read:" user-id)])
                                message-ids)
          results (app-metrics/with-redis metrics :hget_batch
                    #(car/wcar (redis-lib/conn redis)
                               (mapv (fn [[k f]] (car/hget k f)) keys-and-fields)))]
      (zipmap message-ids results))))

(defn entries->decoded [entries user-id]
  (mapv (fn [entry]
          (let [payload (:payload entry)
                message (when payload (decode-message payload))
                sender-id (:sender_id message)
                message-id (:message_id message)
                readable? (and message-id sender-id
                               (not= sender-id user-id))]
            {:message-id message-id :readable? readable?})) entries))

(defn decoded->result [decoded receipts unread]
  (reduce
   (fn [{:keys [unread]} {:keys [message-id readable?]}]
     (if-not readable?
       {:unread unread :done? false}
       (if (get receipts message-id)
         (reduced {:unread unread :done? true})
         {:unread (inc unread) :done? false})))
   {:unread unread :done? false}
   decoded))

(defn unread-count-from-redis
  [redis metrics naming conversation-id user-id]
  (when (and redis naming conversation-id user-id)
    (let [stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conversation-id)
          batch-size 100]
      (loop [cursor nil
             unread 0]
        (let [{:keys [entries next-cursor]} (streams/read! redis metrics stream
                                                           {:direction :backward
                                                            :limit batch-size
                                                            :cursor cursor})
              decoded (entries->decoded entries user-id)
              readable-ids (->> decoded (filter :readable?) (mapv :message-id))
              receipts (batch-receipt-read? redis metrics naming
                                            conversation-id readable-ids user-id)
              result (decoded->result decoded receipts unread)]
          (cond
            (:done? result) (:unread result)
            (empty? entries) (:unread result)
            (nil? next-cursor) (:unread result)
            :else (recur next-cursor (:unread result))))))))

(defn conversation-item-fallback
  "Returns a conversation item with last_message nil and unread_count 0 (e.g. on timeout)."
  [row user-id member-ids profiles]
  (let [base (conversation-row->item row)
        members (build-member-items member-ids profiles)
        counterpart (when (direct-conversation? (:type base))
                      (build-counterpart member-ids profiles user-id))]
    (cond-> (assoc base
                   :members members
                   :last_message nil
                   :unread_count 0)
      counterpart (assoc :counterpart counterpart))))

(defn conversation-item
  [{:keys [redis naming metrics logger] :as components} row user-id member-ids profiles]
  (let [conv-id (:id row)
        _ (when logger (logger/log logger ::conversation-item-start {:conv-id conv-id}))
        t0 (System/nanoTime)
        stream (when (and naming conv-id)
                 (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id))
        redis-msg (redis-last-message redis metrics stream)
        _ (when logger (logger/log logger ::conversation-item-after-redis {:conv-id conv-id
                                                                           :duration-ms (quot (- (System/nanoTime) t0) 1000000)}))
        minio-msg (minio-last-message components conv-id)
        _ (when logger (logger/log logger ::conversation-item-after-minio {:conv-id conv-id}))
        t1 (System/nanoTime)
        unread-count (or (unread-count-from-redis redis metrics naming conv-id user-id) 0)
        _ (when logger (logger/log logger ::conversation-item-after-unread {:conv-id conv-id
                                                                            :duration-ms (quot (- (System/nanoTime) t1) 1000000)}))
        last-message (or redis-msg minio-msg)
        base (conversation-row->item row)
        members (build-member-items member-ids profiles)
        counterpart (when (direct-conversation? (:type base))
                      (build-counterpart member-ids profiles user-id))
        item (cond-> (assoc base
                            :members members
                            :last_message last-message
                            :unread_count unread-count)
               counterpart (assoc :counterpart counterpart))]
    (when logger (logger/log logger ::conversation-item-done {:conv-id conv-id}))
    item))
