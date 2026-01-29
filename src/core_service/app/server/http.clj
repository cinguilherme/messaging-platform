(ns core-service.app.server.http
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [malli.core :as m]
            [malli.error :as me]))

(defn get-accept-format
  "Determines response format from Accept header or defaults to JSON."
  [req]
  (let [accept (get-in req [:headers "accept"] "application/json")]
    (cond
      (re-find #"application/edn" accept) :edn
      (re-find #"application/json" accept) :json
      :else :json)))

(defn format-response
  "Formats response data based on requested format."
  [data format]
  (case format
    :edn {:status 200
          :headers {"content-type" "application/edn"}
          :body (pr-str data)}
    :json {:status 200
           :headers {"content-type" "application/json"}
           :body (json/generate-string data)}))

(defn param
  [req k]
  (or (get-in req [:query-params k])
      (get-in req [:params k])
      (get-in req [:params (keyword k)])
      (get-in req [:path-params k])
      (get-in req [:path-params (keyword k)])
      (get-in req [:route-params k])
      (get-in req [:route-params (keyword k)])))

(defn parse-long
  [value default]
  (try
    (if (some? value)
      (Long/parseLong (str value))
      default)
    (catch Exception _
      default)))

(defn normalize-content-type
  [content-type]
  (some-> content-type (str/split #";" 2) first str/trim))

(defn read-json-body
  "Read JSON request body into a Clojure map. Returns {:ok true :data ...}
  or {:ok false :error ...}."
  [req]
  (try
    (let [body (:body req)
          text (cond
                 (string? body) body
                 (some? body) (slurp body)
                 :else nil)]
      (if (or (nil? text) (str/blank? text))
        {:ok false :error "missing body"}
        {:ok true :data (json/parse-string text true)}))
    (catch Exception _
      {:ok false :error "invalid json"})))

(defn invalid-response
  [format schema value]
  (format-response {:ok false
                    :error "invalid body"
                    :details (me/humanize (m/explain schema value))}
                   format))

(defn parse-uuid
  [value]
  (try
    (when (some? value)
      (java.util.UUID/fromString (str value)))
    (catch Exception _
      nil)))
