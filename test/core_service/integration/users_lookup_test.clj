(ns core-service.integration.users-lookup-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.users.v1.authed :as users]
            [core-service.integration.helpers :as helpers]
            [d-core.core.auth.token-client :as token-client]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(deftest users-lookup-missing-email
  (let [handler (users/users-lookup {:webdeps {:db :dummy
                                               :token-client :dummy
                                               :keycloak {:admin-url "http://keycloak"}}})
        resp (helpers/invoke-handler handler {:request-method :get
                       :headers {"accept" "application/json"}
                       :query-params {}})
        body (json/parse-string (:body resp) true)]
    (testing "missing email"
      (is (= 400 (:status resp)))
      (is (= false (:ok body)))
      (is (= "missing email or username" (:error body))))))

(deftest users-lookup-missing-backend
  (let [handler (users/users-lookup {:webdeps {:db nil
                                               :token-client nil
                                               :keycloak nil}})
        resp (helpers/invoke-handler handler {:request-method :get
                       :headers {"accept" "application/json"}
                       :query-params {"email" "user@example.com"}})
        body (json/parse-string (:body resp) true)]
    (testing "backend not configured"
      (is (= 500 (:status resp)))
      (is (= false (:ok body)))
      (is (= "lookup backend not configured" (:error body))))))

(deftest users-lookup-success-shape
  (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "token"})
                http-client/get (fn [_ _]
                                  {:status 200
                                   :body (json/generate-string
                                          [{:id "user-1"
                                            :email "user@example.com"
                                            :username "user"
                                            :firstName "User"
                                            :lastName "Example"
                                            :enabled true}])})]
    (let [handler (users/users-lookup {:webdeps {:db :dummy
                                                 :token-client :dummy
                                                 :keycloak {:admin-url "http://keycloak"}}})
          resp (helpers/invoke-handler handler {:request-method :get
                         :headers {"accept" "application/json"}
                         :query-params {"email" "USER@EXAMPLE.COM"}})
          body (json/parse-string (:body resp) true)
          item (first (:items body))]
      (testing "response shape"
        (is (= 200 (:status resp)))
        (is (:ok body))
        (is (= "user-1" (:user_id item)))
        (is (= "user@example.com" (:email item)))
        (is (= "user" (:username item)))
        (is (= "User" (:first_name item)))
        (is (= "Example" (:last_name item)))
        (is (= true (:enabled item)))))))

(deftest users-lookup-by-username-local
  (let [{:keys [db client]} (helpers/init-db)
        handler (users/users-lookup {:webdeps {:db db
                                               :token-client nil
                                               :keycloak nil}})
        user-id (java.util.UUID/randomUUID)]
    (try
      (users-db/upsert-user-profile! db {:user-id user-id
                                         :username "alice"
                                         :first-name "Alice"
                                         :last-name "Example"
                                         :email "alice@example.com"
                                         :enabled true})
      (let [resp (helpers/invoke-handler handler {:request-method :get
                           :headers {"accept" "application/json"}
                           :query-params {"username" "@alice"}})
            body (json/parse-string (:body resp) true)
            item (first (:items body))]
        (testing "username lookup"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= "alice" (:username item)))
          (is (= (str user-id) (:user_id item)))))
      (finally
        (sql/delete! db {:table :user_profiles
                         :where {:user_id user-id}})
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))
