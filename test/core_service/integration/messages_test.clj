(ns core-service.integration.messages-test
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.databases]
            [core-service.app.config.messaging]
            [core-service.app.server.conversation.v1.authed :as authed]
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

(defn- latest-stream-entry
  [redis-client stream]
  (first (car/wcar (:conn redis-client)
           (car/xrevrange stream "+" "-" "COUNT" 1))))

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

(deftest message-create-writes-stream
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            handler (authed/messages-create {:db db
                                             :redis redis-client
                                             :naming naming})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
            seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
            payload (json/generate-string {:type "text" :body {:text "hi"}})]
        (try
          ;; ensure membership exists for authorization
          (sql/insert! db {:id conv-id
                           :tenant_id "tenant-1"
                           :type "direct"
                           :title "Test"}
                       {:table :conversations})
          (sql/insert! db {:conversation_id conv-id
                           :user_id sender-id
                           :role "member"}
                       {:table :memberships})
          ;; clear any previous data for this stream key
          (car/wcar (:conn redis-client)
            (car/del stream)
            (car/del seq-key))
          (let [resp (handler {:request-method :post
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
            (car/wcar (:conn redis-client)
              (car/del stream)
              (car/del seq-key))
            (sql/delete! db {:table :memberships
                             :where {:conversation_id conv-id
                                     :user_id sender-id}})
            (sql/delete! db {:table :conversations
                             :where {:id conv-id}})
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest message-read-pagination
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            create-handler (authed/messages-create {:db db
                                                    :redis redis-client
                                                    :naming naming})
            list-handler (authed/messages-list {:db db
                                                :redis redis-client
                                                :naming naming})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            stream (str (get-in naming [:redis :stream-prefix] "chat:conv:") conv-id)
            seq-key (str (get-in naming [:redis :sequence-prefix] "chat:seq:") conv-id)
            payload-one (json/generate-string {:type "text" :body {:text "one"}})
            payload-two (json/generate-string {:type "text" :body {:text "two"}})]
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
            (car/del seq-key))
          (create-handler {:request-method :post
                           :headers {"accept" "application/json"}
                           :params {:id (str conv-id)}
                           :body payload-one
                           :auth/principal {:subject (str sender-id)
                                            :tenant-id "tenant-1"}})
          (create-handler {:request-method :post
                           :headers {"accept" "application/json"}
                           :params {:id (str conv-id)}
                           :body payload-two
                           :auth/principal {:subject (str sender-id)
                                            :tenant-id "tenant-1"}})
          (let [resp (list-handler {:request-method :get
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
              (is (= "two" (get-in msg [:body :text])))
              (is (string? cursor)))
            (let [resp2 (list-handler {:request-method :get
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
            (car/wcar (:conn redis-client)
              (car/del stream)
              (car/del seq-key))
            (sql/delete! db {:table :memberships
                             :where {:conversation_id conv-id
                                     :user_id sender-id}})
            (sql/delete! db {:table :conversations
                             :where {:id conv-id}})
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
