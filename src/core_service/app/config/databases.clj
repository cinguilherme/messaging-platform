(ns core-service.app.config.databases
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [integrant.core :as ig]))

(defn- getenv
  "Returns env var value if present and not blank."
  [k]
  (let [v (some-> (System/getenv k) str/trim)]
    (when (seq v) v)))

(defn- in-docker?
  []
  (or (boolean (getenv "DOCKER"))
      (boolean (getenv "IN_DOCKER"))
      (.exists (io/file "/.dockerenv"))))

(defn- env-true?
  [k]
  (let [v (some-> (getenv k) str/lower-case)]
    (contains? #{"1" "true" "yes" "on"} v)))

(defn- default-host
  [docker-host local-host]
  (if (in-docker?) docker-host local-host))

(defmethod ig/init-key :core-service.app.config.databases/postgres
  [_ {:keys [jdbc-url username password pool? pool]}]
  {:jdbc-url (or jdbc-url
                 (getenv "POSTGRES_URI")
                 (str "jdbc:postgresql://" (default-host "postgres" "localhost") ":5432/core-service"))
   :username (or username (getenv "POSTGRES_USER") "postgres")
   :password (or password (getenv "POSTGRES_PASSWORD") "postgres")
   :pool? (if (nil? pool?) true (boolean pool?))
   :pool pool})

(defmethod ig/init-key :core-service.app.config.databases/ensure-messaging-tables
  [_ {:keys [ensure?]}]
  (if (nil? ensure?)
    (env-true? "DB_BOOTSTRAP")
    (boolean ensure?)))

(defmethod ig/init-key :core-service.app.config.databases/run-migrations
  [_ {:keys [run?]}]
  (if (nil? run?)
    (env-true? "DB_MIGRATIONS")
    (boolean run?)))
