(ns core-service.app.server.conversation.v1.authed.logic
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [core-service.app.db.users :as users-db]
            [core-service.app.executors.protocol :as executor]
            [core-service.app.libs.identity :as identity]
            [core-service.app.libs.time :as time]
            [core-service.app.libs.util :as util]
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
      (update :type util/coerce-keyword)
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
  (identity/user-id-from-request req))

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

(def ^:private default-profile-hydration
  {:max-concurrency 8
   :total-timeout-ms 600
   :request-timeout-ms 250})

(defn- positive-long
  [value default]
  (let [v (cond
            (number? value) (long value)
            (string? value) (try
                              (Long/parseLong value)
                              (catch Exception _
                                nil))
            :else nil)]
    (if (and (some? v) (pos? v))
      v
      default)))

(defn- profile-hydration-config
  [keycloak]
  (let [cfg (:profile-hydration keycloak)]
    {:max-concurrency (positive-long (:max-concurrency cfg) (:max-concurrency default-profile-hydration))
     :total-timeout-ms (positive-long (:total-timeout-ms cfg) (:total-timeout-ms default-profile-hydration))
     :request-timeout-ms (positive-long (:request-timeout-ms cfg) (:request-timeout-ms default-profile-hydration))}))

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

(defn- with-http-timeout-defaults
  [http-opts request-timeout-ms]
  (let [request-timeout-ms (int (max 1 (long request-timeout-ms)))
        http-opts (or http-opts {})]
    (cond-> http-opts
      (nil? (:conn-timeout http-opts)) (assoc :conn-timeout request-timeout-ms)
      (nil? (:socket-timeout http-opts)) (assoc :socket-timeout request-timeout-ms))))

(defn- fetch-keycloak-profiles-bounded
  [{:keys [admin-url headers http-opts fetch-executor max-concurrency timeout-ms]} user-ids]
  (let [timeout-ms (long (max 1 timeout-ms))
        http-opts (with-http-timeout-defaults http-opts timeout-ms)
        tasks (keep (fn [user-id]
                      (try
                        {:user-id user-id
                         :task (executor/execute fetch-executor
                                                 (fn []
                                                   (fetch-keycloak-profile admin-url headers http-opts user-id)))}
                        (catch Exception _
                          nil)))
                    user-ids)]
    (reduce (fn [acc {:keys [task]}]
              (let [result (executor/wait-for task timeout-ms ::timeout)]
                (if (= result ::timeout)
                  (do
                    (executor/cancel task)
                    (update acc :timed-out inc))
                  (if (map? result)
                    (-> acc
                        (update :profiles merge result)
                        (update :fetched + (count result)))
                    acc))))
            {:profiles {}
             :fetched 0
             :timed-out 0
             :max-concurrency max-concurrency}
            tasks)))

(defn fetch-keycloak-profiles
  [{:keys [token-client keycloak keycloak-fetch-executor logger]} user-ids]
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
              headers {"authorization" (str "Bearer " access-token)}
              {:keys [max-concurrency total-timeout-ms request-timeout-ms]} (profile-hydration-config keycloak)
              started-at (System/currentTimeMillis)]
          (if keycloak-fetch-executor
            (loop [remaining user-ids
                   acc-profiles {}
                   acc-fetched 0
                   acc-timed-out 0]
              (let [elapsed (- (System/currentTimeMillis) started-at)
                    remaining-budget (- total-timeout-ms elapsed)]
                (if (or (empty? remaining) (<= remaining-budget 0))
                  (do
                    (when logger
                      (logger/log logger :info ::keycloak-profile-hydration
                                  {:requested (count user-ids)
                                   :fetched acc-fetched
                                   :timed_out acc-timed-out
                                   :duration-ms (- (System/currentTimeMillis) started-at)
                                   :max-concurrency max-concurrency
                                   :total-timeout-ms total-timeout-ms
                                   :request-timeout-ms request-timeout-ms
                                   :bounded? true}))
                    acc-profiles)
                  (let [batch (vec (take max-concurrency remaining))
                        batch-timeout (long (max 1 (min remaining-budget request-timeout-ms)))
                        {:keys [profiles fetched timed-out]}
                        (fetch-keycloak-profiles-bounded {:admin-url admin-url
                                                          :headers headers
                                                          :http-opts http-opts
                                                          :fetch-executor keycloak-fetch-executor
                                                          :max-concurrency max-concurrency
                                                          :timeout-ms batch-timeout}
                                                         batch)]
                    (recur (drop (count batch) remaining)
                           (merge acc-profiles profiles)
                           (+ acc-fetched fetched)
                           (+ acc-timed-out timed-out))))))
            (let [http-opts (with-http-timeout-defaults http-opts request-timeout-ms)
                  profiles (->> user-ids
                                (pmap (partial fetch-keycloak-profile admin-url headers http-opts))
                                (apply merge))]
              (when logger
                (logger/log logger :info ::keycloak-profile-hydration
                            {:requested (count user-ids)
                             :fetched (count profiles)
                             :timed_out 0
                             :duration-ms (- (System/currentTimeMillis) started-at)
                             :max-concurrency max-concurrency
                             :total-timeout-ms total-timeout-ms
                             :request-timeout-ms request-timeout-ms
                             :bounded? false}))
              profiles)))
        (catch Exception _
          {})))))

(defn- persist-fallback-profiles!
  [db fallback-profiles]
  (try
    (let [profiles (->> fallback-profiles
                        (map (fn [[user-id profile]]
                               {:user-id user-id
                                :username (:username profile)
                                :first-name (:first_name profile)
                                :last-name (:last_name profile)
                                :avatar-url (:avatar_url profile)
                                :email (:email profile)
                                :enabled (:enabled profile)}))
                        vec)]
      (users-db/upsert-user-profiles! db profiles))
    (catch Exception _
      nil)))

(defn- schedule-fallback-profile-persist!
  [db executor-pool fallback-profiles]
  (when (and executor-pool (seq fallback-profiles))
    (try
      (executor/execute executor-pool
                        (fn []
                          (persist-fallback-profiles! db fallback-profiles)))
      (catch Exception _
        nil))))

(defn resolve-member-profiles
  ([db token-client keycloak persist-executor member-ids]
   (resolve-member-profiles db token-client keycloak persist-executor nil nil member-ids))
  ([db token-client keycloak persist-executor keycloak-fetch-executor member-ids]
   (resolve-member-profiles db token-client keycloak persist-executor keycloak-fetch-executor nil member-ids))
  ([db token-client keycloak persist-executor keycloak-fetch-executor logger member-ids]
   (let [local-profiles (users-db/fetch-user-profiles db {:user-ids member-ids})
         profiles (profiles-by-id local-profiles)
         missing-ids (remove #(contains? profiles (str %)) member-ids)
         fallback-profiles (fetch-keycloak-profiles {:token-client token-client
                                                     :keycloak keycloak
                                                     :keycloak-fetch-executor keycloak-fetch-executor
                                                     :logger logger}
                                                    missing-ids)]
     (schedule-fallback-profile-persist! db persist-executor fallback-profiles)
     (merge profiles fallback-profiles))))

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

(def ^:private fallback-unread-cache-ttl-ms 30000)
(defonce ^:private fallback-unread-cache (atom {}))

(defn- cache-key
  [conversation-id user-id]
  [(str conversation-id) (str user-id)])

(defn- fallback-cache-get
  [conversation-id user-id]
  (let [k (cache-key conversation-id user-id)
        now (time/now-ms)
        cached (get @fallback-unread-cache k)]
    (when (some? cached)
      (if (> (:expires-at cached) now)
        (:unread cached)
        (do
          (swap! fallback-unread-cache dissoc k)
          nil)))))

(defn- fallback-cache-put!
  [conversation-id user-id unread]
  (swap! fallback-unread-cache assoc (cache-key conversation-id user-id)
         {:unread unread
          :expires-at (+ (time/now-ms) fallback-unread-cache-ttl-ms)})
  unread)

(defn- fallback-cache-evict!
  [conversation-id user-id]
  (swap! fallback-unread-cache dissoc (cache-key conversation-id user-id)))

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
    (if-some [index-count (unread-index/unread-count {:redis redis
                                                      :naming naming
                                                      :metrics metrics}
                                                     conversation-id user-id)]
      (do
        (fallback-cache-evict! conversation-id user-id)
        index-count)
      (or (fallback-cache-get conversation-id user-id)
          (when-some [scanned (unread-count-from-redis-scan streams redis metrics naming conversation-id user-id)]
            (fallback-cache-put! conversation-id user-id scanned))))))

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
