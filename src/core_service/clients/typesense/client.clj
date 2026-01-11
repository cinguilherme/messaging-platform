(ns core-service.clients.typesense.client
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str])
  (:import (java.net URLEncoder)))

(defrecord TypesenseClient [endpoint api-key http-opts]
  Object
  (toString [_] (str "#TypesenseClient{:endpoint " (pr-str endpoint) "}")))

(defn- normalize-endpoint [endpoint]
  (let [s (str endpoint)]
    (if (str/ends-with? s "/")
      (subs s 0 (dec (count s)))
      s)))

(defn make-client
  [{:keys [endpoint api-key timeout-ms]
    :or {endpoint "http://localhost:8108"
         api-key "typesense"
         timeout-ms 5000}}]
  (->TypesenseClient
    (normalize-endpoint endpoint)
    api-key
    {:socket-timeout timeout-ms
     :conn-timeout timeout-ms
     ;; We'll handle non-2xx ourselves so smoke tests can treat 404/409 cleanly.
     :throw-exceptions false}))

(defn- ->query-string [query]
  (when (seq query)
    (str/join
      "&"
      (for [[k v] query
            :let [k (name k)
                  v (if (coll? v) (str/join "," (map str v)) (str v))]]
        (str (URLEncoder/encode k "UTF-8") "=" (URLEncoder/encode v "UTF-8"))))))

(defn request!
  "Low-level Typesense request helper.

  - method: :get/:post/:put/:delete
  - path: e.g. \"/collections\"
  - opts: {:query {...} :body {...} :headers {...}}

  Returns {:status int :headers map :body parsed-or-string}."
  [^TypesenseClient client method path {:keys [query body headers] :as _opts}]
  (let [endpoint (:endpoint client)
        url (str endpoint (if (str/starts-with? path "/") path (str "/" path))
                 (when-let [qs (->query-string query)]
                   (str "?" qs)))
        req (merge
              (:http-opts client)
              {:method method
               :url url
               :headers (merge {"X-TYPESENSE-API-KEY" (:api-key client)
                                "Accept" "application/json"}
                               (or headers {}))}
              (when (some? body)
                {:content-type :json
                 :body (json/generate-string body)}))
        resp (http/request req)
        body-str (:body resp)
        parsed (try
                 (when (and body-str (string? body-str) (not (str/blank? body-str)))
                   (json/parse-string body-str true))
                 (catch Exception _e
                   nil))]
    {:status (:status resp)
     :headers (:headers resp)
     :body (or parsed body-str)}))

(defn get! [client path opts] (request! client :get path opts))
(defn post! [client path opts] (request! client :post path opts))
(defn put! [client path opts] (request! client :put path opts))
(defn delete! [client path opts] (request! client :delete path opts))

