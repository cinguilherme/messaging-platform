(ns core-service.app.server.interceptors.json-body
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [integrant.core :as ig])
  (:import [java.io ByteArrayInputStream]))

(defn- json-content-type?
  [request]
  (some-> (get-in request [:headers "content-type"])
          str/lower-case
          (str/includes? "application/json")))

(defn- read-body-text
  [body]
  (cond
    (nil? body) nil
    (string? body) body
    (bytes? body) (String. ^bytes body "UTF-8")
    :else (slurp body)))

(defn- rewind-body
  [text]
  (when text
    (ByteArrayInputStream. (.getBytes ^String text "UTF-8"))))

(defn json-body-interceptor
  []
  {:name ::json-body
   :enter (fn [ctx]
            (let [request (:request ctx)]
              (if (or (not (json-content-type? request))
                      (contains? request :body-params))
                ctx
                (let [text (read-body-text (:body request))
                      parsed (when (and (string? text) (not (str/blank? text)))
                               (try
                                 (json/parse-string text true)
                                 (catch Exception _ nil)))
                      request (cond-> request
                                (some? text) (assoc :body (rewind-body text))
                                (some? parsed) (assoc :body-params parsed))]
                  (assoc ctx :request request)))))})

(defmethod ig/init-key :core-service.app.server.interceptors.json-body/json-body
  [_ _]
  (json-body-interceptor))
