(ns core-service.app.server.conversation.v1.authed.logic
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [core-service.app.db.users :as users-db]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.server.http :as http]
            [core-service.app.server.message.logic :as message-logic]
            [core-service.app.server.receipt.logic :as receipt-logic]
            [d-core.core.stream.protocol :as p-stream]
            [d-core.core.auth.token-client :as token-client]
            [duct.logger :as logger]))

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
  [streams stream]
  (when (and streams stream)
    (let [{:keys [entries]} (p-stream/read-payloads streams stream {:direction :backward :limit 1})
          payload (:payload (first entries))]
      (when payload
        (message-logic/decode-message payload)))))

(defn minio-last-message
  [{:keys [db minio segments] :as components} conversation-id]
  (when minio
    (let [segments (or segments {})
          result (segment-reader/fetch-messages components conversation-id {:limit 1 :cursor nil :direction :backward})
          messages (vec (:messages result))]
      (first messages))))

(defn entries->decoded
  [entries user-id]
  (mapv (fn [entry]
          (let [payload (:payload entry)
                message (when payload (message-logic/decode-message payload))
                sender-id (:sender_id message)
                message-id (:message_id message)
                readable? (and message-id sender-id
                               (not= sender-id user-id))]
            {:message-id message-id :readable? readable?}))
        entries))

(defn decoded->result
  [decoded receipts unread]
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
  [streams redis metrics naming conversation-id user-id]
  (when (and streams redis naming conversation-id user-id)
    (let [stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conversation-id)
          batch-size 100]
      (loop [cursor nil
             unread 0]
        (let [{:keys [entries next-cursor]} (p-stream/read-payloads streams stream
                                                                     {:direction :backward
                                                                      :limit batch-size
                                                                      :cursor cursor})
              decoded (entries->decoded entries user-id)
              readable-ids (->> decoded (filter :readable?) (mapv :message-id))
              receipts (receipt-logic/batch-receipt-read? redis metrics naming
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
  [{:keys [streams redis naming metrics logger] :as components} row user-id member-ids profiles]
  (let [conv-id (:id row)
        _ (when logger (logger/log logger ::conversation-item-start {:conv-id conv-id}))
        t0 (System/nanoTime)
        stream (when (and naming conv-id)
                 (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id))
        redis-msg (redis-last-message streams stream)
        _ (when logger (logger/log logger ::conversation-item-after-redis {:conv-id conv-id
                                                                           :duration-ms (quot (- (System/nanoTime) t0) 1000000)}))
        minio-msg (minio-last-message components conv-id)
        _ (when logger (logger/log logger ::conversation-item-after-minio {:conv-id conv-id}))
        t1 (System/nanoTime)
        unread-count (or (unread-count-from-redis streams redis metrics naming conv-id user-id) 0)
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
