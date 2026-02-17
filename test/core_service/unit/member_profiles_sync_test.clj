(ns core-service.unit.member-profiles-sync-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.executors.protocol :as executor]
            [core-service.app.libs.executor]
            [core-service.app.server.conversation.v1.authed.logic :as logic]
            [d-core.core.auth.token-client :as token-client]
            [integrant.core :as ig]))

(defn- uuid-from-user-url
  [url]
  (last (str/split url #"/")))

(deftest resolve-member-profiles-schedules-one-batched-persist-with-mapped-keys
  (let [member-id (java.util.UUID/randomUUID)
        persisted (atom nil)
        execute-calls (atom 0)]
    (with-redefs [users-db/fetch-user-profiles (fn [_ _] [])
                  token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                  http-client/get (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:username "alice"
                                             :firstName "Alice"
                                             :lastName "Example"
                                             :email "alice@example.com"
                                             :enabled true
                                             :attributes {:avatar_url "https://example.com/a.png"}})})
                  executor/execute (fn [_ task-fn]
                                     (swap! execute-calls inc)
                                     (task-fn)
                                     :task)
                  users-db/upsert-user-profiles! (fn [_ profiles]
                                                   (reset! persisted profiles)
                                                   :ok)]
      (let [profiles (logic/resolve-member-profiles :db
                                                    :token-client
                                                    {:admin-url "http://keycloak"}
                                                    :executor
                                                    [member-id])
            profile (get profiles (str member-id))]
        (testing "response still includes keycloak fallback profile"
          (is (= "alice" (:username profile)))
          (is (= "Alice" (:first_name profile)))
          (is (= "Example" (:last_name profile)))
          (is (= "https://example.com/a.png" (:avatar_url profile))))
        (testing "persistence is scheduled once and mapped to DB key style"
          (is (= 1 @execute-calls))
          (is (= [{:user-id (str member-id)
                   :username "alice"
                   :first-name "Alice"
                   :last-name "Example"
                   :avatar-url "https://example.com/a.png"
                   :email "alice@example.com"
                   :enabled true}]
                 @persisted)))))))

(deftest resolve-member-profiles-does-not-persist-when-no-fallback
  (let [member-id (java.util.UUID/randomUUID)
        execute-calls (atom 0)
        persist-calls (atom 0)]
    (with-redefs [users-db/fetch-user-profiles (fn [_ _] [])
                  executor/execute (fn [_ _]
                                     (swap! execute-calls inc)
                                     :task)
                  users-db/upsert-user-profiles! (fn [& _]
                                                   (swap! persist-calls inc)
                                                   :ok)]
      (let [profiles (logic/resolve-member-profiles :db nil nil :executor [member-id])]
        (is (= {} profiles))
        (is (= 0 @execute-calls))
        (is (= 0 @persist-calls))))))

(deftest resolve-member-profiles-with-nil-executor-returns-fallback-without-persisting
  (let [member-id (java.util.UUID/randomUUID)
        persist-calls (atom 0)]
    (with-redefs [users-db/fetch-user-profiles (fn [_ _] [])
                  token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                  http-client/get (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:username "bob"
                                             :firstName "Bob"
                                             :lastName "Builder"
                                             :email "bob@example.com"
                                             :enabled true})})
                  users-db/upsert-user-profiles! (fn [& _]
                                                   (swap! persist-calls inc)
                                                   :ok)]
      (let [profiles (logic/resolve-member-profiles :db
                                                    :token-client
                                                    {:admin-url "http://keycloak"}
                                                    nil
                                                    [member-id])]
        (is (= "bob" (get-in profiles [(str member-id) :username])))
        (is (= 0 @persist-calls))))))

(deftest fetch-keycloak-profiles-respects-max-concurrency
  (let [fetch-executor (ig/init-key :core-service.app.libs.executor/keycloak-profile-fetch-executor
                                    {:thread-count 8})
        user-ids (repeatedly 12 #(str (java.util.UUID/randomUUID)))
        in-flight (atom 0)
        max-in-flight (atom 0)]
    (try
      (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                    http-client/get (fn [url _]
                                      (let [active (swap! in-flight inc)
                                            _ (swap! max-in-flight max active)
                                            user-id (uuid-from-user-url url)]
                                        (Thread/sleep 40)
                                        (swap! in-flight dec)
                                        {:status 200
                                         :body (json/generate-string
                                                {:id user-id
                                                 :username (str "u-" user-id)})}))]
        (let [profiles (logic/fetch-keycloak-profiles {:token-client :token-client
                                                        :keycloak {:admin-url "http://keycloak"
                                                                   :profile-hydration {:max-concurrency 3
                                                                                       :total-timeout-ms 3000
                                                                                       :request-timeout-ms 300}}
                                                        :keycloak-fetch-executor fetch-executor}
                                                       user-ids)]
          (is (= (count user-ids) (count profiles)))
          (is (<= @max-in-flight 3))))
      (finally
        (ig/halt-key! :core-service.app.libs.executor/keycloak-profile-fetch-executor fetch-executor)))))

(deftest fetch-keycloak-profiles-returns-partial-results-when-budget-expires
  (let [fetch-executor (ig/init-key :core-service.app.libs.executor/keycloak-profile-fetch-executor
                                    {:thread-count 8})
        user-ids (repeatedly 10 #(str (java.util.UUID/randomUUID)))]
    (try
      (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                    http-client/get (fn [url _]
                                      (let [user-id (uuid-from-user-url url)]
                                        (Thread/sleep 70)
                                        {:status 200
                                         :body (json/generate-string
                                                {:id user-id
                                                 :username (str "u-" user-id)})}))]
        (let [profiles (logic/fetch-keycloak-profiles {:token-client :token-client
                                                        :keycloak {:admin-url "http://keycloak"
                                                                   :profile-hydration {:max-concurrency 4
                                                                                       :total-timeout-ms 130
                                                                                       :request-timeout-ms 90}}
                                                        :keycloak-fetch-executor fetch-executor}
                                                       user-ids)]
          (is (pos? (count profiles)))
          (is (< (count profiles) (count user-ids)))))
      (finally
        (ig/halt-key! :core-service.app.libs.executor/keycloak-profile-fetch-executor fetch-executor)))))

(deftest fetch-keycloak-profiles-dedupes-ids
  (let [fetch-executor (ig/init-key :core-service.app.libs.executor/keycloak-profile-fetch-executor
                                    {:thread-count 4})
        user-a (str (java.util.UUID/randomUUID))
        user-b (str (java.util.UUID/randomUUID))
        seen-ids (atom [])]
    (try
      (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                    http-client/get (fn [url _]
                                      (let [user-id (uuid-from-user-url url)]
                                        (swap! seen-ids conj user-id)
                                        {:status 200
                                         :body (json/generate-string
                                                {:id user-id
                                                 :username (str "u-" user-id)})}))]
        (let [profiles (logic/fetch-keycloak-profiles {:token-client :token-client
                                                        :keycloak {:admin-url "http://keycloak"
                                                                   :profile-hydration {:max-concurrency 4
                                                                                       :total-timeout-ms 1000
                                                                                       :request-timeout-ms 150}}
                                                        :keycloak-fetch-executor fetch-executor}
                                                       [user-a user-a user-b user-b])]
          (is (= 2 (count profiles)))
          (is (= #{user-a user-b} (set @seen-ids)))))
      (finally
        (ig/halt-key! :core-service.app.libs.executor/keycloak-profile-fetch-executor fetch-executor)))))

(deftest fetch-keycloak-profiles-failure-path-returns-empty
  (let [fetch-executor (ig/init-key :core-service.app.libs.executor/keycloak-profile-fetch-executor
                                    {:thread-count 4})]
    (try
      (with-redefs [token-client/client-credentials (fn [_ _] {:access-token "admin-token"})
                    http-client/get (fn [& _]
                                      (throw (ex-info "boom" {})))]
        (is (= {}
               (logic/fetch-keycloak-profiles {:token-client :token-client
                                               :keycloak {:admin-url "http://keycloak"
                                                          :profile-hydration {:max-concurrency 2
                                                                              :total-timeout-ms 100
                                                                              :request-timeout-ms 50}}
                                               :keycloak-fetch-executor fetch-executor}
                                              [(str (java.util.UUID/randomUUID))]))))
      (finally
        (ig/halt-key! :core-service.app.libs.executor/keycloak-profile-fetch-executor fetch-executor)))))
