(ns core-service.integration.user-profiles-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.auth.v1.public.public :as public]
            [core-service.app.server.users.v1.authed :as users]
            [core-service.integration.helpers :as helpers]
            [d-core.core.auth.token-client :as token-client]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]
            [next.jdbc.result-set :as rs]))

(defn- cleanup-user!
  [db user-id]
  (sql/delete! db {:table :user_profiles
                   :where {:user_id user-id}}))

(defn- fetch-updated-at
  [db user-id]
  (-> (sql/execute! db
                    ["SELECT updated_at FROM user_profiles WHERE user_id = ?" user-id]
                    {:builder-fn rs/as-unqualified-lower-maps})
      first
      :updated_at))

(deftest users-lookup-by-ids-returns-items
  (let [{:keys [db client]} (helpers/init-db)
        handler (users/users-lookup-by-ids {:webdeps {:db db}})
        user-id (java.util.UUID/randomUUID)]
    (try
      (users-db/upsert-user-profile! db {:user-id user-id
                                         :username "alice"
                                         :first-name "Alice"
                                         :last-name "Example"
                                         :avatar-url "https://example.com/a.png"
                                         :email "alice@example.com"
                                         :enabled true})
      (let [resp (helpers/invoke-handler handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :body (json/generate-string {:ids [(str user-id)]})})
            body (json/parse-string (:body resp) true)
            item (first (:items body))]
        (testing "response shape"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str user-id) (:user_id item)))
          (is (= "alice" (:username item)))))
      (finally
        (cleanup-user! db user-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest upsert-user-profiles-batches-and-updates-existing-rows
  (let [{:keys [db client]} (helpers/init-db)
        user-a (java.util.UUID/randomUUID)
        user-b (java.util.UUID/randomUUID)]
    (try
      (users-db/upsert-user-profiles! db [{:user-id user-a
                                           :username "alice"
                                           :first-name "Alice"
                                           :last-name "One"
                                           :avatar-url "https://example.com/a.png"
                                           :email "alice@example.com"
                                           :enabled true}
                                          {:user-id user-b
                                           :username "bob"
                                           :first-name "Bob"
                                           :last-name "Two"
                                           :avatar-url "https://example.com/b.png"
                                           :email "bob@example.com"
                                           :enabled true}])
      (let [initial-a (users-db/fetch-user-profile db {:user-id user-a})
            initial-b (users-db/fetch-user-profile db {:user-id user-b})
            initial-a-updated-at (fetch-updated-at db user-a)]
        (testing "initial insert persists all rows"
          (is (= "alice" (:username initial-a)))
          (is (= "Alice" (:first_name initial-a)))
          (is (= "One" (:last_name initial-a)))
          (is (= "https://example.com/a.png" (:avatar_url initial-a)))
          (is (= "alice@example.com" (:email initial-a)))
          (is (= true (:enabled initial-a)))
          (is (= "bob" (:username initial-b)))
          (is (= "Bob" (:first_name initial-b)))
          (is (= "Two" (:last_name initial-b))))
        (Thread/sleep 5)
        (users-db/upsert-user-profiles! db [{:user-id user-a
                                             :username "alice-updated"
                                             :first-name "Alice2"
                                             :last-name "Updated"
                                             :avatar-url "https://example.com/a2.png"
                                             :email "alice2@example.com"
                                             :enabled false}])
        (let [updated-a (users-db/fetch-user-profile db {:user-id user-a})
              updated-a-updated-at (fetch-updated-at db user-a)
              unchanged-b (users-db/fetch-user-profile db {:user-id user-b})]
          (testing "upsert updates existing row and touches updated_at"
            (is (= "alice-updated" (:username updated-a)))
            (is (= "Alice2" (:first_name updated-a)))
            (is (= "Updated" (:last_name updated-a)))
            (is (= "https://example.com/a2.png" (:avatar_url updated-a)))
            (is (= "alice2@example.com" (:email updated-a)))
            (is (= false (:enabled updated-a)))
            (is (> (.getTime updated-a-updated-at) (.getTime initial-a-updated-at))))
          (testing "other rows remain intact"
            (is (= "bob" (:username unchanged-b)))
            (is (= "Bob" (:first_name unchanged-b)))
            (is (= "Two" (:last_name unchanged-b))))))
      (finally
        (cleanup-user! db user-a)
        (cleanup-user! db user-b)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest auth-register-upserts-profile
  (let [{:keys [db client]} (helpers/init-db)
        user-id (java.util.UUID/randomUUID)
        handler (public/auth-register {:webdeps {:db db
                                                 :token-client :dummy
                                                 :keycloak {:admin-url "http://keycloak"
                                                            :token-url "http://token"
                                                            :client-id "cid"}}})]
    (try
      (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "token"})
                    http-client/post (fn [_ _]
                                       {:status 201
                                        :headers {"location" (str "http://keycloak/admin/realms/d-core/users/" user-id)}})]
        (let [resp (helpers/invoke-handler handler {:request-method :post
                                                    :headers {"accept" "application/json"}
                                                    :body (json/generate-string {:username "alice"
                                                                                 :password "password"
                                                                                 :email "alice@example.com"
                                                                                 :first_name "Alice"
                                                                                 :last_name "Example"})})
              body (json/parse-string (:body resp) true)
              profile (users-db/fetch-user-profile db {:user-id user-id})]
          (testing "profile persisted"
            (is (= 200 (:status resp)))
            (is (:ok body))
            (is (= "alice" (:username profile)))
            (is (= "Alice" (:first_name profile)))
            (is (= "Example" (:last_name profile)))
            (is (= "alice@example.com" (:email profile))))))
      (finally
        (cleanup-user! db user-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest auth-login-upserts-profile
  (let [{:keys [db client]} (helpers/init-db)
        user-id (java.util.UUID/randomUUID)
        handler (public/auth-login {:webdeps {:db db
                                              :token-client :dummy
                                              :keycloak {:token-url "http://token"
                                                         :client-id "cid"
                                                         :client-secret "secret"
                                                         :admin-url "http://keycloak"}}})]
    (try
      (with-redefs [http-client/post (fn [_ _]
                                       {:status 200
                                        :body (json/generate-string {:access_token "token"
                                                                     :refresh_token "refresh-token"})})
                    token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                    http-client/get (fn [_ _]
                                      {:status 200
                                       :body (json/generate-string
                                              [{:id (str user-id)
                                                :email "alice@example.com"
                                                :username "alice"
                                                :firstName "Alice"
                                                :lastName "Example"
                                                :enabled true}])})]
        (let [resp (helpers/invoke-handler handler {:request-method :post
                                                    :headers {"accept" "application/json"}
                                                    :body (json/generate-string {:username "alice"
                                                                                 :password "password"})})
              body (json/parse-string (:body resp) true)
              profile (users-db/fetch-user-profile db {:user-id user-id})]
          (testing "profile persisted"
            (is (= 200 (:status resp)))
            (is (:ok body))
            (is (= "alice" (:username profile)))
            (is (= "Alice" (:first_name profile)))
            (is (= "Example" (:last_name profile)))
            (is (= "alice@example.com" (:email profile))))
          (testing "token response includes refresh token"
            (is (= "token" (get-in body [:token :access_token])))
            (is (= "refresh-token" (get-in body [:token :refresh_token]))))))
      (finally
        (cleanup-user! db user-id)
        (ig/halt-key! :d-core.core.clients.postgres/client client)))))

(deftest auth-refresh-returns-token
  (let [handler (public/auth-refresh {:webdeps {:keycloak {:token-url "http://token"
                                                           :client-id "cid"
                                                           :client-secret "secret"}}})
        seen (atom nil)]
    (with-redefs [http-client/post (fn [url opts]
                                     (reset! seen {:url url :opts opts})
                                     {:status 200
                                      :body (json/generate-string {:access_token "new-token"
                                                                   :refresh_token "new-refresh"})})]
      (let [resp (helpers/invoke-handler handler {:request-method :post
                                                  :headers {"accept" "application/json"}
                                                  :body (json/generate-string {:refresh_token "refresh-123"
                                                                               :scope "openid"})})
            body (json/parse-string (:body resp) true)
            {:keys [url opts]} @seen]
        (is (= 200 (:status resp)))
        (is (:ok body))
        (is (= "new-token" (get-in body [:token :access_token])))
        (is (= "new-refresh" (get-in body [:token :refresh_token])))
        (is (= "http://token" url))
        (is (= "refresh_token" (get-in opts [:form-params :grant_type])))
        (is (= "refresh-123" (get-in opts [:form-params :refresh_token])))
        (is (= "openid" (get-in opts [:form-params :scope])))
        (is (= "cid" (get-in opts [:form-params :client_id])))
        (is (= "secret" (get-in opts [:form-params :client_secret])))))))
