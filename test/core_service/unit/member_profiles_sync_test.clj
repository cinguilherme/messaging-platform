(ns core-service.unit.member-profiles-sync-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.db.users :as users-db]
            [core-service.app.executors.protocol :as executor]
            [core-service.app.server.conversation.v1.authed.logic :as logic]
            [d-core.core.auth.token-client :as token-client]))

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
