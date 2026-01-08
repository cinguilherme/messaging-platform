(ns core-service.routes
  (:require [cheshire.core :as json]))

(defn- get-accept-format
  "Determines response format from Accept header or defaults to JSON"
  [req]
  (let [accept (get-in req [:headers "accept"] "application/json")]
    (cond
      (re-find #"application/edn" accept) :edn
      (re-find #"application/json" accept) :json
      :else :json)))

(defn- format-response
  "Formats response data based on requested format"
  [data format]
  (case format
    :edn {:status 200
          :headers {"content-type" "application/edn"}
          :body (pr-str data)}
    :json {:status 200
           :headers {"content-type" "application/json"}
           :body (json/generate-string data)}))

(defn index [_options]
  (fn [_req]
    (format-response {:ok true} (get-accept-format _req))))

(defn test [_options]
  (fn [_req]
    (println _options)
    (format-response {:ok true :message "Hello, World!"} (get-accept-format _req))))