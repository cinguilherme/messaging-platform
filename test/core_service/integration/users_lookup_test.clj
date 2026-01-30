(ns core-service.integration.users-lookup-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.server.users.v1.authed :as users]
            [d-core.core.auth.token-client :as token-client]))

(deftest users-lookup-missing-email
  (let [handler (users/users-lookup {:token-client :dummy
                                     :keycloak {:admin-url "http://keycloak"}})
        resp (handler {:request-method :get
                       :headers {"accept" "application/json"}
                       :query-params {}})
        body (json/parse-string (:body resp) true)]
    (testing "missing email"
      (is (= 200 (:status resp)))
      (is (= false (:ok body)))
      (is (= "missing email" (:error body))))))

(deftest users-lookup-missing-backend
  (let [handler (users/users-lookup {:token-client nil
                                     :keycloak nil})
        resp (handler {:request-method :get
                       :headers {"accept" "application/json"}
                       :query-params {"email" "user@example.com"}})
        body (json/parse-string (:body resp) true)]
    (testing "backend not configured"
      (is (= 200 (:status resp)))
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
    (let [handler (users/users-lookup {:token-client :dummy
                                       :keycloak {:admin-url "http://keycloak"}})
          resp (handler {:request-method :get
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
