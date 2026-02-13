(ns core-service.unit.auth-require-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.server.auth]
            [integrant.core :as ig]))

(deftest require-fn-uses-read-scope-for-attachment-head
  (let [require-fn (ig/init-key :core-service.app.server.auth/require-fn {})]
    (testing "HEAD attachment route resolves to messages:read"
      (is (= {:scopes #{"messages:read"}}
             (require-fn {:request-method :head
                          :uri "/v1/conversations/11111111-1111-1111-1111-111111111111/attachments/22222222-2222-2222-2222-222222222222"}))))))
