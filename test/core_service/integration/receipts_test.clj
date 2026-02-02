(ns core-service.integration.receipts-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.server.conversation.v1.authed]
            [core-service.app.redis.receipts :as receipts]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [integrant.core :as ig]
            [core-service.app.libs.util :as util]
            [taoensso.carmine :as car]))

(deftest receipts-expire-with-ttl
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            receipt-config (assoc (ig/init-key :core-service.app.config.messaging/receipt-config {})
                                  :ttl-ms 2000)
            conversation-id (util/random-uuid)
            message-id (util/random-uuid)
            user-id (util/random-uuid)
            key (receipts/receipt-key naming conversation-id message-id)]
        (try
          (receipts/record! {:redis redis-client
                             :naming naming
                             :receipt receipt-config}
                            {:conversation-id conversation-id
                             :message-id message-id
                             :user-id user-id
                             :receipt-type :read})
          (testing "ttl is set on receipt key"
            (let [ttl (car/wcar (:conn redis-client) (car/pttl key))]
              (is (pos? ttl))
              (is (<= ttl 2000))))
          (finally
            (car/wcar (:conn redis-client) (car/del key))))))))

(deftest receipt-endpoint-records
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            receipt-config (ig/init-key :core-service.app.config.messaging/receipt-config {})
            handler (core-service.app.server.conversation.v1.authed/receipts-create
                      {:webdeps {:db db
                                 :redis redis-client
                                 :naming naming
                                 :receipt receipt-config}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            message-id (util/random-uuid)
            key (receipts/receipt-key naming conv-id message-id)]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [resp (handler {:request-method :post
                               :headers {"accept" "application/json"}
                               :params {:id (str conv-id)}
                               :body (json/generate-string {:receipt_type "read"
                                                            :message_id (str message-id)})
                               :auth/principal {:subject (str sender-id)
                                                :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)]
            (testing "receipt endpoint writes to redis"
              (is (= 200 (:status resp)))
              (is (:ok body))
              (is (= (str message-id) (:message_id body)))
              (is (= "read" (:receipt_type body)))
              (is (seq (receipts/get-receipts {:redis redis-client
                                               :naming naming}
                                              {:conversation-id conv-id
                                               :message-id message-id})))))
          (finally
            (car/wcar (:conn redis-client) (car/del key))
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
