(ns core-service.integration.segments-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.config.storage]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.storage.minio :as minio]
            [core-service.app.server.conversation.v1.authed :as authed]
            [core-service.app.workers.segments :as segments]
            [core-service.app.workers.segment-retention :as retention]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.storage.minio]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(deftest segment-flush-writes-object-and-index
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
            handler (authed/messages-create {:webdeps {:db db
                                                       :redis redis-client
                                                       :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            emoji-text "hello 👋🌍"
            long-text (str (apply str (repeat 120 "a")) " 😀😅")
            payload-one (json/generate-string {:type "text" :body {:text emoji-text}})
            payload-two (json/generate-string {:type "text" :body {:text long-text}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
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
            (helpers/cleanup-segment-object-and-index! db minio-client conv-id)
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest segment-retention-deletes-old-segments
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
            retention-config (ig/init-key :core-service.app.config.messaging/retention-config {})
            handler (authed/messages-create {:webdeps {:db db
                                                       :redis redis-client
                                                       :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            payload (json/generate-string {:type "text" :body {:text "old 🕰️"}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (handler {:request-method :post
                    :headers {"accept" "application/json"}
                    :params {:id (str conv-id)}
                    :body payload
                    :auth/principal {:subject (str sender-id)
                                     :tenant-id "tenant-1"}})
          (segments/flush-conversation! {:db db
                                         :redis redis-client
                                         :minio minio-client
                                         :naming naming
                                         :segments segment-config
                                         :logger nil}
                                        conv-id)
          (let [row (first (sql/select db {:table :segment_index
                                           :where {:conversation_id conv-id}}))
                object-key (:object_key row)
                cutoff (java.sql.Timestamp. (- (System/currentTimeMillis) 1000))]
            (sql/execute! db ["UPDATE segment_index SET created_at = ? WHERE conversation_id = ?"
                              cutoff conv-id]
                          {})
            (retention/cleanup! {:db db
                                 :minio minio-client
                                 :retention (assoc retention-config :max-age-ms 1)
                                 :logger nil})
            (testing "segment index removed"
              (is (empty? (sql/select db {:table :segment_index
                                          :where {:conversation_id conv-id}}))))
            (testing "segment object deleted"
              (let [obj (minio/get-bytes! minio-client object-key)]
                (is (false? (:ok obj))))))
          (finally
            (helpers/cleanup-segment-object-and-index! db minio-client conv-id)
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest segment-flush-trims-stream
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            segment-config (assoc (ig/init-key :core-service.app.config.messaging/segment-config {})
                                  :trim-stream? true
                                  :trim-min-entries 2)
            handler (authed/messages-create {:webdeps {:db db
                                                       :redis redis-client
                                                       :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            {:keys [stream]} (helpers/redis-keys naming conv-id)
            payloads [(json/generate-string {:type "text" :body {:text "one"}})
                      (json/generate-string {:type "text" :body {:text "two"}})
                      (json/generate-string {:type "text" :body {:text "three"}})
                      (json/generate-string {:type "text" :body {:text "four"}})]]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (doseq [payload payloads]
            (handler {:request-method :post
                      :headers {"accept" "application/json"}
                      :params {:id (str conv-id)}
                      :body payload
                      :auth/principal {:subject (str sender-id)
                                       :tenant-id "tenant-1"}}))
          (segments/flush-conversation! {:db db
                                         :redis redis-client
                                         :minio minio-client
                                         :naming naming
                                         :segments segment-config
                                         :logger nil}
                                        conv-id)
          (testing "stream trimmed to min entries"
            (is (= 2 (helpers/stream-len redis-client stream))))
          (finally
            (helpers/cleanup-segment-object-and-index! db minio-client conv-id)
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
