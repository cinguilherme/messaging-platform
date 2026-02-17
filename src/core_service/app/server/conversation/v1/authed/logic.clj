(ns core-service.app.server.conversation.v1.authed.logic
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [core-service.app.db.users :as users-db]
            [core-service.app.executors.protocol :as executor]
            [core-service.app.redis.conversation-last :as conversation-last]
            [core-service.app.redis.unread-index :as unread-index]
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

(defn fetch-keycloak-profile
  [admin-url headers http-opts user-id]
  (try
    (let [resp (http-client/get (str admin-url "/users/" user-id)
                                (merge {:headers headers
                                        :as :text
                                        :throw-exceptions false}
                                       http-opts))
          status (:status resp)
          parsed (some-> (:body resp) (json/parse-string true))]
      (if (<= 200 status 299)
        {user-id (keycloak-user->profile parsed)}
        {}))
    (catch Exception _
      {})))

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
          (->> user-ids
               (pmap (partial fetch-keycloak-profile admin-url headers http-opts))
               (apply merge)))
        (catch Exception _
          {})))))

(defn- persist-fallback-profiles!
  [db fallback-profiles]
  (try
    (doseq [[user-id profile] fallback-profiles]
      (users-db/upsert-user-profile! db (assoc profile :user-id user-id)))
    (catch Exception _
      nil)))

(defn resolve-member-profiles
  [db token-client keycloak executor-pool member-ids]
  (let [local-profiles (users-db/fetch-user-profiles db {:user-ids member-ids})
        profiles (profiles-by-id local-profiles)
        missing-ids (remove #(contains? profiles (str %)) member-ids)
        fallback-profiles (fetch-keycloak-profiles {:token-client token-client
                                                    :keycloak keycloak}
                                                   missing-ids)]
    (when (seq fallback-profiles)
      (executor/execute executor-pool
                        (fn []
                          (persist-fallback-profiles! db fallback-profiles))))
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

(defn last-messages-by-conversation
  [{:keys [redis naming metrics]} conversation-ids]
  (conversation-last/batch-last-messages {:redis redis
                                          :naming naming
                                          :metrics metrics}
                                         conversation-ids))

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

(defn unread-count-from-redis-scan
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

(defn unread-count-from-redis
  [streams redis metrics naming conversation-id user-id]
  (when (and streams redis naming conversation-id user-id)
    (or (unread-index/unread-count {:redis redis
                                    :naming naming
                                    :metrics metrics}
                                   conversation-id user-id)
        (unread-count-from-redis-scan streams redis metrics naming conversation-id user-id))))

(defn conversation-item-fallback
  "Returns a fallback conversation item (e.g. on timeout) with unread_count 0."
  [row user-id member-ids profiles last-messages-by-conv]
  (let [base (conversation-row->item row)
        members (build-member-items member-ids profiles)
        conv-id (:id row)
        last-message (get last-messages-by-conv conv-id)
        counterpart (when (direct-conversation? (:type base))
                      (build-counterpart member-ids profiles user-id))]
    (cond-> (assoc base
                   :members members
                   :last_message last-message
                   :unread_count 0)
      counterpart (assoc :counterpart counterpart))))

(defn conversation-item
  [{:keys [streams redis naming metrics logger]} row user-id member-ids profiles last-messages-by-conv]
  (let [conv-id (:id row)
        _ (when logger (logger/log logger ::conversation-item-start {:conv-id conv-id}))
        t0 (System/nanoTime)
        last-message (get last-messages-by-conv conv-id)
        _ (when logger (logger/log logger ::conversation-item-after-last-msg {:conv-id conv-id
                                                                              :duration-ms (quot (- (System/nanoTime) t0) 1000000)}))
        t1 (System/nanoTime)
        unread-count (or (unread-count-from-redis streams redis metrics naming conv-id user-id) 0)
        _ (when logger (logger/log logger ::conversation-item-after-unread {:conv-id conv-id
                                                                            :duration-ms (quot (- (System/nanoTime) t1) 1000000)}))
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
