(ns core-service.integration.messages-test
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.config.storage]
            [core-service.app.server.conversation.v1.authed.authed :as authed]
            [core-service.app.pagination :as pagination]
            [core-service.app.workers.segments :as segments]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.storage.minio]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn- latest-stream-entry
  [redis-client stream]
  (first (car/wcar (:conn redis-client)
                   (car/xrevrange stream "+" "-" "COUNT" 1))))

(deftest message-create-writes-stream
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            handler (authed/messages-create {:webdeps {:db db
                                                       :redis redis-client
                                                       :streams streams
                                                       :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            {:keys [stream seq-key]} (helpers/redis-keys naming conv-id)
            payload (json/generate-string {:type "text" :body {:text "hi"}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [resp (helpers/invoke-handler handler {:request-method :post
                                                      :headers {"accept" "application/json"}
                                                      :params {:id (str conv-id)}
                                                      :body payload
                                                      :auth/principal {:subject (str sender-id)
                                                                       :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)
                entry (latest-stream-entry redis-client stream)]
            (testing "handler response"
              (is (= 200 (:status resp)))
              (is (:ok body))
              (is (= (str conv-id) (:conversation_id body))))
            (testing "redis stream entry exists"
              (is entry "expected stream entry")
              (let [[_id fields] entry
                    payload (get (apply hash-map fields) "payload")
                    payload-text (if (bytes? payload)
                                   (String. ^bytes payload "UTF-8")
                                   (str payload))
                    message (edn/read-string payload-text)]
                (is (= (str conv-id) (str (:conversation_id message))))
                (is (= "hi" (get-in message [:body :text]))))))
          (finally
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest message-read-pagination
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            create-handler (authed/messages-create {:webdeps {:db db
                                                              :redis redis-client
                                                              :streams streams
                                                              :naming naming}})
            list-handler (authed/messages-list {:webdeps {:db db
                                                          :redis redis-client
                                                          :streams streams
                                                          :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            payload-one (json/generate-string {:type "text" :body {:text "one"}})
            payload-two (json/generate-string {:type "text" :body {:text "two 😀"}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-one
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-two
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (let [resp (helpers/invoke-handler list-handler {:request-method :get
                                                           :headers {"accept" "application/json"}
                                                           :params {:id (str conv-id)}
                                                           :query-params {"limit" "1"}
                                                           :auth/principal {:subject (str sender-id)
                                                                            :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)
                msg (first (:messages body))
                cursor (:next_cursor body)]
            (testing "first page returns latest"
              (is (= 200 (:status resp)))
              (is (= "two 😀" (get-in msg [:body :text])))
              (is (string? cursor)))
            (let [resp2 (helpers/invoke-handler list-handler {:request-method :get
                                                              :headers {"accept" "application/json"}
                                                              :params {:id (str conv-id)}
                                                              :query-params {"limit" "1"
                                                                             "cursor" cursor}
                                                              :auth/principal {:subject (str sender-id)
                                                                               :tenant-id "tenant-1"}})
                  body2 (json/parse-string (:body resp2) true)
                  msg2 (first (:messages body2))]
              (testing "second page returns older message"
                (is (= "one" (get-in msg2 [:body :text]))))))
          (finally
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest message-read-cursor-mismatch
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            list-handler (authed/messages-list {:webdeps {:db db
                                                          :redis redis-client
                                                          :streams streams
                                                          :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            other-conv (java.util.UUID/randomUUID)
            bad-cursor (pagination/encode-token
                        {:conversation_id (str other-conv)
                         :cursor "0-0"
                         :direction "backward"
                         :source "redis"})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [resp (helpers/invoke-handler list-handler {:request-method :get
                                                           :headers {"accept" "application/json"}
                                                           :params {:id (str conv-id)}
                                                           :query-params {"limit" "1"
                                                                          "cursor" bad-cursor}
                                                           :auth/principal {:subject (str sender-id)
                                                                            :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)]
            (testing "cursor conversation mismatch"
              (is (= 400 (:status resp)))
              (is (= false (:ok body)))
              (is (= "cursor conversation mismatch" (:error body)))))
          (finally
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest message-read-from-minio
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
            create-handler (authed/messages-create {:webdeps {:db db
                                                              :redis redis-client
                                                              :streams streams
                                                              :naming naming}})
            list-handler (authed/messages-list {:webdeps {:db db
                                                          :redis redis-client
                                                          :streams streams
                                                          :minio minio-client
                                                          :naming naming
                                                          :segments segment-config}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            payload-one (json/generate-string {:type "text" :body {:text "one 🧩"}})
            payload-two (json/generate-string {:type "text" :body {:text "two 🚀"}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-one
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-two
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (segments/flush-conversation! {:db db
                                         :redis redis-client
                                         :streams streams
                                         :minio minio-client
                                         :naming naming
                                         :segments segment-config
                                         :logger nil}
                                        conv-id)
          ;; simulate older history living only in Minio
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [resp (helpers/invoke-handler list-handler {:request-method :get
                                                           :headers {"accept" "application/json"}
                                                           :params {:id (str conv-id)}
                                                           :query-params {"limit" "2"}
                                                           :auth/principal {:subject (str sender-id)
                                                                            :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)
                msgs (:messages body)]
            (testing "minio read returns messages"
              (is (= 200 (:status resp)))
              (is (= 2 (count msgs)))
              (is (= "two 🚀" (get-in (first msgs) [:body :text])))
              (is (= "one 🧩" (get-in (second msgs) [:body :text])))))
          (finally
            (helpers/cleanup-segment-object-and-index! db minio-client conv-id)
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest message-read-merge-redis-and-minio
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
            create-handler (authed/messages-create {:webdeps {:db db
                                                              :redis redis-client
                                                              :streams streams
                                                              :naming naming}})
            list-handler (authed/messages-list {:webdeps {:db db
                                                          :redis redis-client
                                                          :streams streams
                                                          :minio minio-client
                                                          :naming naming
                                                          :segments segment-config}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            payload-one (json/generate-string {:type "text" :body {:text "one 🧊"}})
            payload-two (json/generate-string {:type "text" :body {:text "two 🔥"}})
            payload-three (json/generate-string {:type "text" :body {:text "three ✅"}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          ;; first two messages -> flush to Minio
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-one
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-two
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (segments/flush-conversation! {:db db
                                         :redis redis-client
                                         :streams streams
                                         :minio minio-client
                                         :naming naming
                                         :segments segment-config
                                         :logger nil}
                                        conv-id)
          ;; third message stays in Redis
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-three
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (let [resp (helpers/invoke-handler list-handler {:request-method :get
                                                           :headers {"accept" "application/json"}
                                                           :params {:id (str conv-id)}
                                                           :query-params {"limit" "3"}
                                                           :auth/principal {:subject (str sender-id)
                                                                            :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)
                msgs (:messages body)]
            (testing "merge redis + minio returns all messages"
              (is (= 200 (:status resp)))
              (is (= 3 (count msgs)))
              (is (= "three ✅" (get-in (first msgs) [:body :text])))
              (is (= "two 🔥" (get-in (second msgs) [:body :text])))
              (is (= "one 🧊" (get-in (nth msgs 2) [:body :text])))))
          (finally
            (helpers/cleanup-segment-object-and-index! db minio-client conv-id)
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest message-read-from-minio-forward
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
            create-handler (authed/messages-create {:webdeps {:db db
                                                              :redis redis-client
                                                              :streams streams
                                                              :naming naming}})
            list-handler (authed/messages-list {:webdeps {:db db
                                                          :redis redis-client
                                                          :streams streams
                                                          :minio minio-client
                                                          :naming naming
                                                          :segments segment-config}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            payload-one (json/generate-string {:type "text" :body {:text "one ⬆️"}})
            payload-two (json/generate-string {:type "text" :body {:text "two ⬆️"}})
            payload-three (json/generate-string {:type "text" :body {:text "three ⬆️"}})
            cursor (pagination/encode-token {:conversation_id (str conv-id)
                                             :cursor 0
                                             :direction "forward"
                                             :source "minio"})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-one
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-two
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (helpers/invoke-handler create-handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :body payload-three
                                                  :auth/principal {:subject (str sender-id)
                                                                   :tenant-id "tenant-1"}})
          (segments/flush-conversation! {:db db
                                         :redis redis-client
                                         :streams streams
                                         :minio minio-client
                                         :naming naming
                                         :segments segment-config
                                         :logger nil}
                                        conv-id)
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [resp (helpers/invoke-handler list-handler {:request-method :get
                                                           :headers {"accept" "application/json"}
                                                           :params {:id (str conv-id)}
                                                           :query-params {"limit" "2"
                                                                          "cursor" cursor
                                                                          "direction" "forward"}
                                                           :auth/principal {:subject (str sender-id)
                                                                            :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)
                msgs (:messages body)
                next-cursor (:next_cursor body)]
            (testing "minio forward returns oldest first"
              (is (= 200 (:status resp)))
              (is (= 2 (count msgs)))
              (is (= "one ⬆️" (get-in (first msgs) [:body :text])))
              (is (= "two ⬆️" (get-in (second msgs) [:body :text]))))
            (let [resp2 (helpers/invoke-handler list-handler {:request-method :get
                                                              :headers {"accept" "application/json"}
                                                              :params {:id (str conv-id)}
                                                              :query-params {"limit" "2"
                                                                             "cursor" next-cursor
                                                                             "direction" "forward"}
                                                              :auth/principal {:subject (str sender-id)
                                                                               :tenant-id "tenant-1"}})
                  body2 (json/parse-string (:body resp2) true)
                  msgs2 (:messages body2)]
              (testing "minio forward continues"
                (is (= 1 (count msgs2)))
                (is (= "three ⬆️" (get-in (first msgs2) [:body :text]))))))
          (finally
            (helpers/cleanup-segment-object-and-index! db minio-client conv-id)
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
