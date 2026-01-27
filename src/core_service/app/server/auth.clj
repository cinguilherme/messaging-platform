(ns core-service.app.server.auth
  (:require [clojure.string :as str]
            [integrant.core :as ig]))

(defn- public-path?
  [uri]
  (contains? #{"/v1/auth/register" "/v1/auth/login"} uri))

(defn- conversation-path?
  [uri]
  (str/starts-with? uri "/v1/conversations"))

(defn- scope-for-request
  [req]
  (let [method (:request-method req)]
    (case method
      :get {:scopes #{"messages:read"}}
      :post {:scopes #{"messages:write"}}
      :put {:scopes #{"messages:write"}}
      :patch {:scopes #{"messages:write"}}
      :delete {:scopes #{"messages:write"}}
      {:scopes #{"messages:read"}})))

(defmethod ig/init-key :core-service.app.server.auth/require-fn
  [_ _]
  (fn [req]
    (let [uri (:uri req)]
      (cond
        (public-path? uri) nil
        (conversation-path? uri) (scope-for-request req)
        :else nil))))
