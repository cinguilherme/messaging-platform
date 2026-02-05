(ns core-service.app.server.interceptors.api-key
  (:require [integrant.core :as ig]
            [clojure.string :as str]))

(defn api-key-interceptor
  [{:keys [keys header bypass-paths bypass-prefixes]}]
  (let [header (some-> (or header "x-api-key") str/lower-case)
        key-set (set keys)
        bypass-paths (set bypass-paths)
        bypass-prefixes (or bypass-prefixes [])]
    {:name ::api-key
     :enter (fn [ctx]
              (let [req (:request ctx)
                    uri (:uri req)
                    bypass? (or (contains? bypass-paths uri)
                                (some #(str/starts-with? uri %) bypass-prefixes))]
                (if bypass?
                  ctx
                  (if (empty? key-set)
                    (assoc ctx :response {:status 401 :body "API keys not configured"})
                    (let [provided (get-in req [:headers header])]
                      (if (and provided (contains? key-set provided))
                        (update ctx :request assoc :auth/api-key provided)
                        (assoc ctx :response {:status 401 :body "Unauthorized"})))))))}))

(defmethod ig/init-key :core-service.app.server.interceptors.api-key/api-key
  [_ opts]
  (api-key-interceptor opts))
