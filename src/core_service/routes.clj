(ns core-service.routes
  (:refer-clojure :exclude [test])
  (:require [cheshire.core :as json]
            [integrant.core :as ig]
            [core-service.queue :as q]))

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
    (let [queue (:queue _options)
          msg {:message "Hello, World!" :timestamp (System/currentTimeMillis)}]
      (q/enqueue! queue msg)
      (format-response {:ok true
                        :enqueued msg
                        :queue-size (q/size queue)}
                       (get-accept-format _req)))))

;; Hook route keys into Integrant so Duct can instantiate handlers and inject options.
(defmethod ig/init-key :core-service.routes/index
  [_ opts]
  (index opts))

(defmethod ig/init-key :core-service.routes/test
  [_ opts]
  (test opts))