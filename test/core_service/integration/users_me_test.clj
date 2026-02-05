;; Tests for GET /v1/users/me
(ns core-service.integration.users-me-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.users.v1.authed :as users]
            [core-service.app.config.webdeps :as webdeps]
            [core-service.app.server.middleware :as middleware]
            [core-service.integration.helpers :as helpers]
            [duct.logger :as logger]
            [d-core.core.auth.token-client :as token-client]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(defn- cleanup-user!
  [db user-id]
  (sql/delete! db {:table :user_profiles
                   :where {:user_id user-id}}))

(deftest users-me-returns-cached-profile
  (let [{:keys [db client]} (helpers/init-db)
        webdeps (webdeps/map->WebDeps {:db db
                                       :logger nil
                                       :token-client :dummy
                                       :keycloak {:admin-url "http://keycloak"}})
        handler (-> (users/users-me {:webdeps webdeps})
                    middleware/wrap-user-context
                    middleware/wrap-format)
        user-id (java.util.UUID/randomUUID)]
    (try
      (users-db/upsert-user-profile! db {:user-id user-id
                                         :username "alice"
                                         :first-name "Alice"
                                         :last-name "Example"
                                         :email "alice@example.com"
                                         :enabled true})
      (let [resp (handler {:request-method :get
                           :headers {"accept" "application/json"}
                           :auth/principal {:subject (str user-id)}})
            body (json/parse-string (:body resp) true)
            item (:item body)]
        (testing "cached profile response"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str user-id) (:user_id item)))
          (is (= "alice" (:username item)))))
      (finally
        (cleanup-user! db user-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest users-me-fallbacks-to-keycloak-and-caches
  (let [{:keys [db client]} (helpers/init-db)
        webdeps (webdeps/map->WebDeps {:db db
                                       :logger nil
                                       :token-client :dummy
                                       :keycloak {:admin-url "http://keycloak"}})
        handler (-> (users/users-me {:webdeps webdeps})
                    middleware/wrap-user-context
                    middleware/wrap-format)
        user-id (java.util.UUID/randomUUID)]
    (try
      (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "token"})
                    http-client/get (fn [_ _]
                                      {:status 200
                                       :body (json/generate-string
                                              {:id (str user-id)
                                               :email "alice@example.com"
                                               :username "alice"
                                               :firstName "Alice"
                                               :lastName "Example"
                                               :enabled true})})]
        (let [resp (handler {:request-method :get
                             :headers {"accept" "application/json"}
                             :auth/principal {:subject (str user-id)}})
              body (json/parse-string (:body resp) true)
              item (:item body)
              cached (users-db/fetch-user-profile db {:user-id user-id})]
          (testing "keycloak fallback response"
            (is (= 200 (:status resp)))
            (is (:ok body))
            (is (= (str user-id) (:user_id item)))
            (is (= "alice" (:username item))))
          (testing "profile cached locally"
            (is (= "alice" (:username cached)))
            (is (= "Alice" (:first_name cached))))))
      (finally
        (cleanup-user! db user-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))
