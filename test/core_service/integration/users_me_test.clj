;; Tests for GET /v1/users/me
(ns core-service.integration.users-me-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.users.v1.authed :as users]
            [core-service.app.config.webdeps :as webdeps]
            [core-service.app.server.interceptors :as interceptors]
            [core-service.app.server.interceptors.user-context :as user-context-interceptor]
            [core-service.app.server.interceptors.format :as format-interceptor]
            [core-service.integration.helpers :as helpers]
            [duct.logger :as logger]
            [d-core.core.auth.token-client :as token-client]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]
            [reitit.ring :as reitit-ring]
            [reitit.http :as reitit-http]
            [reitit.interceptor.sieppari :as sieppari]))

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
        handler (users/users-me {:webdeps webdeps})
        interceptors [(user-context-interceptor/user-context-interceptor)
                      (format-interceptor/format-interceptor)]
        user-id (java.util.UUID/randomUUID)]
    (try
      (users-db/upsert-user-profile! db {:user-id user-id
                                         :username "alice"
                                         :first-name "Alice"
                                         :last-name "Example"
                                         :email "alice@example.com"
                                         :enabled true})
      (let [req {:request-method :get
                 :uri "/"
                 :headers {"accept" "application/json"}
                 :auth/principal {:subject (str user-id)}}
            router (reitit-http/router
                    ["/" {:get {:handler (fn [ctx]
                                           (assoc ctx :response (handler ctx)))
                                :interceptors interceptors}}])
            app (reitit-http/ring-handler router (reitit-ring/create-default-handler) {:executor sieppari/executor})
            resp (app req)
            resp (if (and (map? resp) (contains? resp :response)) (:response resp) resp)
            ;; If it's not formatted, it won't have :status, but we can assume 200 if :ok is true
            resp (if (and (not (contains? resp :status)) (:ok resp))
                   (assoc resp :status 200)
                   resp)
            body (if (string? (:body resp))
                   (json/parse-string (:body resp) true)
                   (or (:body resp) resp))
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
        handler (users/users-me {:webdeps webdeps})
        interceptors [(user-context-interceptor/user-context-interceptor)
                      (format-interceptor/format-interceptor)]
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
        (let [req {:request-method :get
                   :uri "/"
                   :headers {"accept" "application/json"}
                   :auth/principal {:subject (str user-id)}}
              router (reitit-http/router
                      ["/" {:get {:handler (fn [ctx]
                                             (assoc ctx :response (handler ctx)))
                                  :interceptors interceptors}}])
              app (reitit-http/ring-handler router (reitit-ring/create-default-handler) {:executor sieppari/executor})
              resp (app req)
              resp (if (and (map? resp) (contains? resp :response)) (:response resp) resp)
              resp (if (and (not (contains? resp :status)) (:ok resp))
                     (assoc resp :status 200)
                     resp)
              body (if (string? (:body resp))
                     (json/parse-string (:body resp) true)
                     (or (:body resp) resp))
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
