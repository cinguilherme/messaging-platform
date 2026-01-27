(ns core-service.integration.segments-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.databases]
            [core-service.app.config.messaging]
            [core-service.app.config.storage]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.storage.minio :as minio]
            [core-service.app.server.conversation.v1.authed :as authed]
            [core-service.app.workers.segments :as segments]
            [d-core.core.clients.redis]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.clients.postgres]
            [d-core.core.databases.postgres]
            [d-core.core.databases.sql.common]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn- redis-up?
  [redis-client]
  (try
    (let [resp (car/wcar (:conn redis-client) (car/ping))]
      (= "PONG" resp))
    (catch Exception _ false)))

(defn- minio-up?
  [minio-client]
  (try
    (:ok (minio/list-objects minio-client {:prefix "" :limit 1}))
    (catch Exception _ false)))

(defn- init-db
  []
  (let [pg-cfg (ig/init-key :core-service.app.config.databases/postgres {})
        client (ig/init-key :d-core.core.clients.postgres/client pg-cfg)
        pg-db (ig/init-key :d-core.core.databases.postgres/db {:postgres-client client})
        common (ig/init-key :d-core.core.databases.sql/common
                            {:default-engine :postgres
                             :engines {:postgres pg-db}})]
    {:client client
     :db common}))

(deftest segment-flush-writes-object-and-index
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :core-service.app.storage.minio/client minio-cfg)]
    (if-not (and (redis-up? redis-client) (minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
            handler (authed/messages-create {:db db
                                             :redis redis-client
                                             :naming naming})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
            seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
            flush-key (str (get-in naming [:redis :flush-prefix] "chat:flush:") conv-id)
            emoji-text "hello 👋🌍"
            long-text (str (apply str (repeat 120 "a")) " 😀😅")
            payload-one (json/generate-string {:type "text" :body {:text emoji-text}})
            payload-two (json/generate-string {:type "text" :body {:text long-text}})]
        (try
          (sql/insert! db {:id conv-id
                           :tenant_id "tenant-1"
                           :type "direct"
                           :title "Test"}
                       {:table :conversations})
          (sql/insert! db {:conversation_id conv-id
                           :user_id sender-id
                           :role "member"}
                       {:table :memberships})
          (car/wcar (:conn redis-client)
            (car/del stream)
            (car/del seq-key)
            (car/del flush-key))
          (handler {:request-method :post
                    :headers {"accept" "application/json"}
                    :params {:id (str conv-id)}
                    :body payload-one
                    :auth/principal {:subject (str sender-id)
                                     :tenant-id "tenant-1"}})
          (handler {:request-method :post
                    :headers {"accept" "application/json"}
                    :params {:id (str conv-id)}
                    :body payload-two
                    :auth/principal {:subject (str sender-id)
                                     :tenant-id "tenant-1"}})
          (let [result (segments/flush-conversation! {:db db
                                                      :redis redis-client
                                                      :minio minio-client
                                                      :naming naming
                                                      :segments segment-config
                                                      :logger nil}
                                                     conv-id)
                rows (sql/select db {:table :segment_index
                                     :where {:conversation_id conv-id}})
                row (first rows)]
            (testing "flush result"
              (is (= :ok (:status result)))
              (is (= 1 (count rows))))
            (testing "segment index row"
              (is (= 1 (:seq_start row)))
              (is (= 2 (:seq_end row)))
              (is (string? (:object_key row))))
            (when-let [object-key (:object_key row)]
              (let [obj (minio/get-bytes! minio-client object-key)
                    decoded (segment-format/decode-segment (:bytes obj)
                                                           {:compression (:compression segment-config)
                                                            :codec (:codec segment-config)})
                    messages (:messages decoded)]
                (testing "segment payload"
                  (is (:ok obj))
                  (is (= emoji-text (get-in (first messages) [:body :text])))
                  (is (= long-text (get-in (second messages) [:body :text])))))))
          (finally
            (when-let [row (first (sql/select db {:table :segment_index
                                                  :where {:conversation_id conv-id}}))]
              (minio/delete-object! minio-client (:object_key row))
              (sql/delete! db {:table :segment_index
                               :where {:conversation_id conv-id
                                       :seq_start (:seq_start row)}}))
            (car/wcar (:conn redis-client)
              (car/del stream)
              (car/del seq-key)
              (car/del flush-key))
            (sql/delete! db {:table :memberships
                             :where {:conversation_id conv-id
                                     :user_id sender-id}})
            (sql/delete! db {:table :conversations
                             :where {:id conv-id}})
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
