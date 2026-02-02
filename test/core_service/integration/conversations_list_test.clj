(ns core-service.integration.conversations-list-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.server.conversation.v1.authed :as authed]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(deftest conversations-list-requires-sender
  (let [{:keys [db client]} (helpers/init-db)
        handler (authed/conversations-list {:webdeps {:db db}})]
    (try
      (let [resp (handler {:request-method :get
                           :headers {"accept" "application/json"}})
            body (json/parse-string (:body resp) true)]
        (testing "missing sender id"
          (is (= 200 (:status resp)))
          (is (= false (:ok body)))
          (is (= "invalid sender id" (:error body)))))
      (finally
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest conversations-list-returns-items
  (let [{:keys [db client]} (helpers/init-db)
        handler (authed/conversations-list {:webdeps {:db db}})
        sender-id (java.util.UUID/randomUUID)
        conv-id (java.util.UUID/randomUUID)]
    (try
      (helpers/setup-conversation! db {:conversation-id conv-id
                                       :user-id sender-id
                                       :title "Test"})
      (let [resp (handler {:request-method :get
                           :headers {"accept" "application/json"}
                           :auth/principal {:subject (str sender-id)}})
            body (json/parse-string (:body resp) true)
            item (first (:items body))]
        (testing "response shape"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str conv-id) (:conversation_id item)))
          (is (contains? item :updated_at))
          (is (contains? item :last_message))
          (is (contains? item :unread_count))
          (is (some #(= (str sender-id) (:user_id %)) (:members item)))))
      (finally
        (helpers/cleanup-conversation! db conv-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest conversations-list-paginates-with-cursor
  (let [{:keys [db client]} (helpers/init-db)
        handler (authed/conversations-list {:webdeps {:db db}})
        sender-id (java.util.UUID/randomUUID)
        conv-old (java.util.UUID/randomUUID)
        conv-new (java.util.UUID/randomUUID)
        ts-old 1700000000000
        ts-new 1700000005000]
    (try
      (helpers/setup-conversation! db {:conversation-id conv-old
                                       :user-id sender-id
                                       :title "Old"})
      (helpers/setup-conversation! db {:conversation-id conv-new
                                       :user-id sender-id
                                       :title "New"})
      (sql/execute! db ["UPDATE conversations SET created_at = ? WHERE id = ?"
                        (java.sql.Timestamp. ts-old)
                        conv-old]
                    {})
      (sql/execute! db ["UPDATE conversations SET created_at = ? WHERE id = ?"
                        (java.sql.Timestamp. ts-new)
                        conv-new]
                    {})
      (let [resp (handler {:request-method :get
                           :headers {"accept" "application/json"}
                           :query-params {"limit" "1"}
                           :auth/principal {:subject (str sender-id)}})
            body (json/parse-string (:body resp) true)
            item (first (:items body))
            cursor (:next_cursor body)]
        (testing "first page returns newest"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str conv-new) (:conversation_id item)))
          (is (= (str ts-new) cursor)))
        (let [resp2 (handler {:request-method :get
                              :headers {"accept" "application/json"}
                              :query-params {"limit" "1"
                                             "cursor" cursor}
                              :auth/principal {:subject (str sender-id)}})
              body2 (json/parse-string (:body resp2) true)
              item2 (first (:items body2))]
          (testing "second page returns older"
            (is (= 200 (:status resp2)))
            (is (:ok body2))
            (is (= (str conv-old) (:conversation_id item2))))))
      (finally
        (helpers/cleanup-conversation! db conv-old)
        (helpers/cleanup-conversation! db conv-new)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest conversations-list-invalid-cursor
  (let [{:keys [db client]} (helpers/init-db)
        handler (authed/conversations-list {:webdeps {:db db}})
        sender-id (java.util.UUID/randomUUID)]
    (try
      (let [resp (handler {:request-method :get
                           :headers {"accept" "application/json"}
                           :query-params {"cursor" "nope"}
                           :auth/principal {:subject (str sender-id)}})
            body (json/parse-string (:body resp) true)]
        (testing "invalid cursor"
          (is (= 200 (:status resp)))
          (is (= false (:ok body)))
          (is (= "invalid cursor" (:error body)))))
      (finally
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest conversations-list-last-message-and-unread
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            idempotency (ig/init-key :core-service.app.config.messaging/idempotency-config {})
            receipt (ig/init-key :core-service.app.config.messaging/receipt-config {})
            create-handler (authed/messages-create {:webdeps {:db db
                                                              :redis redis-client
                                                              :naming naming
                                                              :idempotency idempotency}})
            receipt-handler (authed/receipts-create {:webdeps {:db db
                                                               :redis redis-client
                                                               :naming naming
                                                               :receipt receipt}})
            list-handler (authed/conversations-list {:webdeps {:db db
                                                               :redis redis-client
                                                               :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            receiver-id (java.util.UUID/randomUUID)
            payload (json/generate-string {:type "text"
                                           :body {:text "hello"}})]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/ensure-membership! db {:conversation-id conv-id
                                          :user-id receiver-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [resp (create-handler {:request-method :post
                                      :headers {"accept" "application/json"
                                                "idempotency-key" (str (java.util.UUID/randomUUID))}
                                      :params {:id (str conv-id)}
                                      :body payload
                                      :auth/principal {:subject (str sender-id)
                                                       :tenant-id "tenant-1"}})
                body (json/parse-string (:body resp) true)
                message-id (get-in body [:message :message_id])]
            (testing "list shows last_message and unread_count before receipt"
              (let [resp1 (list-handler {:request-method :get
                                         :headers {"accept" "application/json"}
                                         :auth/principal {:subject (str receiver-id)}})
                    body1 (json/parse-string (:body resp1) true)
                    item (first (:items body1))]
                (is (= 200 (:status resp1)))
                (is (:ok body1))
                (is (= "hello" (get-in item [:last_message :body :text])))
                (is (= 1 (:unread_count item)))))
            (receipt-handler {:request-method :post
                              :headers {"accept" "application/json"}
                              :params {:id (str conv-id)}
                              :body (json/generate-string {:receipt_type "read"
                                                           :message_id message-id})
                              :auth/principal {:subject (str receiver-id)
                                               :tenant-id "tenant-1"}})
            (testing "list shows unread_count 0 after read receipt"
              (let [resp2 (list-handler {:request-method :get
                                         :headers {"accept" "application/json"}
                                         :auth/principal {:subject (str receiver-id)}})
                    body2 (json/parse-string (:body resp2) true)
                    item2 (first (:items body2))]
                (is (= 200 (:status resp2)))
                (is (:ok body2))
                (is (= "hello" (get-in item2 [:last_message :body :text])))
                (is (= 0 (:unread_count item2))))))
          (finally
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
