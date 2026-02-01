(ns core-service.app.server.auth
  (:require [clojure.string :as str]
            [integrant.core :as ig]))

(defn- public-path?
  [uri]
  (contains? #{"/v1/auth/register" "/v1/auth/login" "/v1/auth/refresh"} uri))

(defn- conversation-root?
  [uri]
  (= "/v1/conversations" uri))

(defn- conversation-id?
  [uri]
  (boolean (re-matches #"^/v1/conversations/[^/]+$" uri)))

(defn- conversation-messages?
  [uri]
  (boolean (re-matches #"^/v1/conversations/[^/]+/messages$" uri)))

(defn- read-scope []
  {:scopes #{"messages:read"}})

(defn- write-scope []
  {:scopes #{"messages:write"}})

(defn- scope-for-conversations
  [method uri]
  (cond
    (and (conversation-root? uri) (= method :post)) (write-scope)
    (conversation-root? uri) (read-scope)
    (and (conversation-id? uri) (= method :get)) (read-scope)
    (conversation-id? uri) (write-scope)
    (and (conversation-messages? uri) (= method :get)) (read-scope)
    (and (conversation-messages? uri) (= method :post)) (write-scope)
    :else nil))

(defmethod ig/init-key :core-service.app.server.auth/require-fn
  [_ _]
  (fn [req]
    (let [uri (:uri req)
          method (:request-method req)]
      (cond
        (public-path? uri) nil
        (str/starts-with? uri "/v1/conversations") (scope-for-conversations method uri)
        :else nil))))
