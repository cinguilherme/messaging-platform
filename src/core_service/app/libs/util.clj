(ns core-service.app.libs.util
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]))

(defn msg->bytes [msg]
  (.getBytes (json/generate-string msg) "UTF-8"))

(defn msg->edn [msg]
  (edn/read-string (String. ^bytes msg "UTF-8")))

(defn msg->json [msg]
  (json/parse-string (String. ^bytes msg "UTF-8") true))

(defn random-uuid []
  (java.util.UUID/randomUUID))