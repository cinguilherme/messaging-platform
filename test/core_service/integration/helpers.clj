(ns core-service.integration.helpers
  (:require [cheshire.core :as json]
            [core-service.app.config.databases]
            [core-service.app.server.http :as http]
            [core-service.app.server.middleware :as middleware]
            [d-core.core.stream.redis.logic :as stream-logic]
            [d-core.core.stream.redis.redis]
            [d-core.core.clients.postgres]
            [d-core.core.storage.protocol :as p-storage]
            [d-core.core.clients.redis]
            [d-core.core.databases.postgres]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.databases.sql.common]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(def ^:private path-param-keys
  #{:id :attachment_id})

(defn- keywordize-map-keys
  [m]
  (if (map? m)
    (reduce-kv (fn [acc k v]
                 (if (string? k)
                   (assoc acc (keyword k) v)
                   (assoc acc k v)))
               {}
               m)
    {}))

(defn- maybe-parse-long
  [value]
  (if (and (string? value) (re-matches #"^-?\d+$" value))
    (try
      (Long/parseLong value)
      (catch Exception _
        value))
    value))

(defn- infer-path-params
  [params]
  (->> path-param-keys
       (reduce (fn [acc k]
                 (let [raw (or (get params k) (get params (name k)))
                       parsed (http/parse-uuid raw)]
                   (if parsed
                     (assoc acc k parsed)
                     acc)))
               {})))

(defn- infer-query-params
  [req params]
  (let [params-query (dissoc params :id "id" :attachment_id "attachment_id")]
    (reduce-kv (fn [acc k v]
                 (assoc acc k (maybe-parse-long v)))
               {}
               (keywordize-map-keys (merge (:query-params req) params-query)))))

(defn- infer-json-body
  [req]
  (let [body (:body req)]
    (cond
      (map? body) body
      (string? body) (try
                       (json/parse-string body true)
                       (catch Exception _
                         nil))
      :else nil)))

(defn- attach-inferred-parameters
  [req]
  (let [params (or (:params req) {})
        existing (or (:parameters req) {})
        inferred-path (infer-path-params params)
        inferred-query (infer-query-params req params)
        inferred-body (infer-json-body req)]
    (assoc req
           :parameters
           (cond-> existing
             (and (seq inferred-path) (empty? (:path existing)))
             (assoc :path inferred-path)
             (and (seq inferred-query) (empty? (:query existing)))
             (assoc :query inferred-query)
             (and (map? inferred-body) (nil? (:body existing)))
             (assoc :body inferred-body)))))

(defn- format-like-interceptor
  [req resp]
  (let [format (http/get-accept-format req)]
    (cond
      (and (map? resp)
           (not (contains? resp :status))
           (not (contains? resp :request))
           (not (:reitit.core/match resp)))
      (http/format-response resp format)

      (and (map? resp)
           (contains? resp :status)
           (map? (:body resp)))
      (let [formatted (http/format-response (:body resp) format)]
        (-> resp
            (assoc :body (:body formatted))
            (update :headers merge (:headers formatted))))

      :else
      resp)))

(defn invoke-handler
  [handler req]
  (let [req' (-> req
                 (update :headers #(merge {"accept" "application/json"} (or % {})))
                 (attach-inferred-parameters))
        response ((middleware/wrap-user-context handler) req')]
    (format-like-interceptor req' response)))

(defn init-db
  []
  (let [pg-cfg (ig/init-key :core-service.app.config.databases/postgres {})
        client (ig/init-key :d-core.core.clients.postgres/client pg-cfg)
        pg-db (ig/init-key :d-core.core.databases.postgres/db {:postgres-client client})
        common (ig/init-key :d-core.core.databases.sql/common
                            {:default-engine :postgres
                             :engines {:postgres pg-db}})]
    {:client client
     :db common}))

(defn redis-up?
  [redis-client]
  (try
    (let [resp (car/wcar (:conn redis-client) (car/ping))]
      (= "PONG" resp))
    (catch Exception _ false)))

(defn minio-up?
  [minio-client]
  (try
    (:ok (p-storage/storage-list minio-client {:prefix "" :limit 1}))
    (catch Exception _ false)))

(defn redis-keys
  [naming conv-id]
  (let [meta-prefix (get-in naming [:redis :stream-meta-prefix] "__dcore:stream")
        seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
        flush-key (str (get-in naming [:redis :flush-prefix] "chat:flush:") conv-id)
        last-message-key (str (get-in naming [:redis :conversation-last-prefix] "chat:conv_last:") conv-id)]
    {:stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
     :seq-key seq-key
     :flush-key flush-key
     :last-message-key last-message-key
     :sequence-hash-key (stream-logic/sequence-hash-key meta-prefix)
     :cursor-hash-key (stream-logic/cursor-hash-key meta-prefix)}))

(defn init-streams-backend
  [redis-client naming]
  (ig/init-key :core-service.app.streams.redis/backend
               {:redis-client redis-client
                :meta-key-prefix (get-in naming [:redis :stream-meta-prefix] "__dcore:stream")}))

(defn clear-redis-conversation!
  [redis-client naming conv-id]
  (let [{:keys [stream seq-key flush-key last-message-key sequence-hash-key cursor-hash-key]} (redis-keys naming conv-id)]
    (car/wcar (:conn redis-client)
              (car/del stream)
              (car/del seq-key)
              (car/del flush-key)
              (car/del last-message-key)
              (car/hdel sequence-hash-key seq-key)
              (car/hdel cursor-hash-key flush-key))))

(defn stream-len
  [redis-client stream]
  (car/wcar (:conn redis-client)
            (car/xlen stream)))

(defn ensure-conversation!
  [db {:keys [conversation-id tenant-id type title]}]
  (sql/insert! db {:id conversation-id
                   :tenant_id (or tenant-id "tenant-1")
                   :type (or type "direct")
                   :title (or title "Test")}
               {:table :conversations}))

(defn ensure-membership!
  [db {:keys [conversation-id user-id role]}]
  (sql/insert! db {:conversation_id conversation-id
                   :user_id user-id
                   :role (or role "member")}
               {:table :memberships}))

(defn setup-conversation!
  [db {:keys [conversation-id user-id tenant-id type title role]}]
  (ensure-conversation! db {:conversation-id conversation-id
                            :tenant-id tenant-id
                            :type type
                            :title title})
  (ensure-membership! db {:conversation-id conversation-id
                          :user-id user-id
                          :role role}))

(defn- missing-attachments-table?
  [e]
  (loop [t e]
    (if-not t
      false
      (let [sql-state (or (some-> (ex-data t) :next.jdbc/sql-state)
                          (when (instance? org.postgresql.util.PSQLException t)
                            (.getSQLState ^org.postgresql.util.PSQLException t)))
            message (some-> (.getMessage ^Throwable t) str)]
        (if (and (= "42P01" sql-state)
                 (or (nil? message)
                     (.contains message "attachments")))
          true
          (recur (.getCause ^Throwable t)))))))

(defn cleanup-conversation!
  ([db conversation-id]
   (cleanup-conversation! db conversation-id nil))
  ([db conversation-id log]
   (try
     (sql/delete! db {:table :attachments
                      :where {:conversation_id conversation-id}})
     (catch Exception e
       (if (missing-attachments-table? e)
         nil
         (do
           (when log
             (logger/log log :error ::cleanup-attachments-failed
                         {:conversation-id conversation-id
                          :error (.getMessage e)}))
           (throw e)))))
   (sql/delete! db {:table :memberships
                    :where {:conversation_id conversation-id}})
   (sql/delete! db {:table :conversations
                    :where {:id conversation-id}})))

(defn cleanup-segment-object-and-index!
  [db minio-client conversation-id]
  (when-let [row (first (sql/select db {:table :segment_index
                                        :where {:conversation_id conversation-id}}))]
    (p-storage/storage-delete minio-client (:object_key row) {})
    (sql/delete! db {:table :segment_index
                     :where {:conversation_id conversation-id
                             :seq_start (:seq_start row)}})))
