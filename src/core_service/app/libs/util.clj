(ns core-service.app.libs.util
  (:refer-clojure :exclude [random-uuid])
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [duct.logger :as logger]))

(defn msg->bytes [msg]
  (.getBytes (json/generate-string msg) "UTF-8"))

(defn msg->edn [msg]
  (edn/read-string (String. ^bytes msg "UTF-8")))

(defn msg->json [msg]
  (json/parse-string (String. ^bytes msg "UTF-8") true))

(defn coerce-keyword
  [value]
  (if (string? value)
    (keyword value)
    value))

(defn decode-edn-payload
  [payload]
  (cond
    (bytes? payload) (edn/read-string (String. ^bytes payload "UTF-8"))
    (string? payload) (edn/read-string payload)
    :else nil))

(defn random-uuid []
  (java.util.UUID/randomUUID))

(defn ltap [logger log-key v]
  (logger/log logger :info log-key v)
  v)