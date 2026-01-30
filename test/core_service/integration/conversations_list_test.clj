(ns core-service.integration.conversations-list-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.server.conversation.v1.authed :as authed]
            [core-service.integration.helpers :as helpers]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(deftest conversations-list-requires-sender
  (let [{:keys [db client]} (helpers/init-db)
        handler (authed/conversations-list {:db db})]
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
        handler (authed/conversations-list {:db db})
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
          (is (contains? item :unread_count))))
      (finally
        (helpers/cleanup-conversation! db conv-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest conversations-list-paginates-with-cursor
  (let [{:keys [db client]} (helpers/init-db)
        handler (authed/conversations-list {:db db})
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
        handler (authed/conversations-list {:db db})
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
