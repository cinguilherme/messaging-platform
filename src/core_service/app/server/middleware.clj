(ns core-service.app.server.middleware
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [ring.middleware.multipart-params :as multipart]))

(defn- normalize-header
  [value]
  (some-> value str/lower-case))

(defn wrap-api-key
  [handler {:keys [keys header bypass-paths bypass-prefixes]}]
  (let [header (normalize-header (or header "x-api-key"))
        key-set (set keys)
        bypass-paths (set bypass-paths)
        bypass-prefixes (or bypass-prefixes [])]
    (fn [req]
      (let [uri (:uri req)
            bypass? (or (contains? bypass-paths uri)
                        (some #(str/starts-with? uri %) bypass-prefixes))]
        (cond
          bypass?
          (handler req)

          (empty? key-set)
          {:status 401 :body "API keys not configured"}

          :else
          (let [provided (get-in req [:headers header])]
            (if (and provided (contains? key-set provided))
              (handler (assoc req :auth/api-key provided))
              {:status 401 :body "Unauthorized"})))))))

(defmethod ig/init-key :core-service.app.server.middleware/api-key
  [_ {:keys [keys header bypass-paths bypass-prefixes]}]
  (fn [handler]
    (wrap-api-key handler {:keys keys
                           :header header
                           :bypass-paths bypass-paths
                           :bypass-prefixes bypass-prefixes})))

(defmethod ig/init-key :core-service.app.server.middleware/multipart
  [_ _]
  multipart/wrap-multipart-params)
