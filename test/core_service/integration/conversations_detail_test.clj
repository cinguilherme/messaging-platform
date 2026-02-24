(ns core-service.integration.conversations-detail-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.libs.executor]
            [core-service.app.server.conversation.v1.authed.authed :as authed]
            [core-service.integration.helpers :as helpers]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(defn- cleanup-user!
  [db user-id]
  (sql/delete! db {:table :user_profiles
                   :where {:user_id user-id}}))

(deftest conversations-detail-returns-members
  (let [{:keys [db client]} (helpers/init-db)
        executor (ig/init-key :core-service.app.libs.executor/executor {:thread-count 4})
        handler (authed/conversations-get {:webdeps {:db db :executor executor}})
        sender-id (java.util.UUID/randomUUID)
        other-id (java.util.UUID/randomUUID)
        conv-id (java.util.UUID/randomUUID)]
    (try
      (helpers/setup-conversation! db {:conversation-id conv-id
                                       :user-id sender-id
                                       :title "Test"})
      (helpers/ensure-membership! db {:conversation-id conv-id
                                      :user-id other-id})
      (users-db/upsert-user-profile! db {:user-id sender-id
                                         :username "alice"
                                         :first-name "Alice"
                                         :last-name "Example"})
      (users-db/upsert-user-profile! db {:user-id other-id
                                         :username "bob"
                                         :first-name "Bob"
                                         :last-name "Example"})
      (let [resp (helpers/invoke-handler handler {:request-method :get
                                                  :headers {"accept" "application/json"}
                                                  :params {:id (str conv-id)}
                                                  :auth/principal {:subject (str sender-id)}})
            body (json/parse-string (:body resp) true)
            item (:item body)
            member-ids (set (map :user_id (:members item)))]
        (testing "response shape"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str conv-id) (:conversation_id item)))
          (is (contains? item :created_at))
          (is (contains? item :updated_at))
          (is (= #{"alice" "bob"} (set (map :username (:members item)))))
          (is (= #{(str sender-id) (str other-id)} member-ids))))
      (finally
        (cleanup-user! db sender-id)
        (cleanup-user! db other-id)
        (helpers/cleanup-conversation! db conv-id)
        (ig/halt-key! :core-service.app.libs.executor/executor executor)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))
