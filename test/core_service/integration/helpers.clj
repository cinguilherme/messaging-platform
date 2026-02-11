(ns core-service.integration.helpers
  (:require [core-service.app.config.databases]
            [d-core.core.stream.redis.logic :as stream-logic]
            [d-core.core.stream.redis.redis]
            [d-core.core.clients.postgres]
            [d-core.core.storage.protocol :as p-storage]
            [d-core.core.clients.redis]
            [d-core.core.databases.postgres]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.databases.sql.common]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

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
        flush-key (str (get-in naming [:redis :flush-prefix] "chat:flush:") conv-id)]
    {:stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
     :seq-key seq-key
     :flush-key flush-key
     :sequence-hash-key (stream-logic/sequence-hash-key meta-prefix)
     :cursor-hash-key (stream-logic/cursor-hash-key meta-prefix)}))

(defn init-streams-backend
  [redis-client naming]
  (ig/init-key :core-service.app.streams.redis/backend
               {:redis-client redis-client
                :meta-key-prefix (get-in naming [:redis :stream-meta-prefix] "__dcore:stream")}))

(defn clear-redis-conversation!
  [redis-client naming conv-id]
  (let [{:keys [stream seq-key flush-key sequence-hash-key cursor-hash-key]} (redis-keys naming conv-id)]
    (car/wcar (:conn redis-client)
      (car/del stream)
      (car/del seq-key)
      (car/del flush-key)
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

(defn cleanup-conversation!
  [db conversation-id]
  (try
    (sql/delete! db {:table :attachments
                     :where {:conversation_id conversation-id}})
    (catch Exception _
      nil))
  (sql/delete! db {:table :memberships
                   :where {:conversation_id conversation-id}})
  (sql/delete! db {:table :conversations
                   :where {:id conversation-id}}))

(defn cleanup-segment-object-and-index!
  [db minio-client conversation-id]
  (when-let [row (first (sql/select db {:table :segment_index
                                        :where {:conversation_id conversation-id}}))]
    (p-storage/storage-delete minio-client (:object_key row) {})
    (sql/delete! db {:table :segment_index
                     :where {:conversation_id conversation-id
                             :seq_start (:seq_start row)}})))
