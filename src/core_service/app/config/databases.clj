(ns core-service.app.config.databases
  (:require [core-service.app.libs.env :as env]
            [integrant.core :as ig]))

(defmethod ig/init-key :core-service.app.config.databases/postgres
  [_ {:keys [jdbc-url username password pool? pool]}]
  {:jdbc-url (or jdbc-url
                 (env/getenv "POSTGRES_URI")
                 (str "jdbc:postgresql://" (env/default-host "postgres" "localhost") ":5432/core-service"))
   :username (or username (env/getenv "POSTGRES_USER") "postgres")
   :password (or password (env/getenv "POSTGRES_PASSWORD") "postgres")
   :pool? (if (nil? pool?) true (boolean pool?))
   :pool pool})

(defmethod ig/init-key :core-service.app.config.databases/ensure-messaging-tables
  [_ {:keys [ensure?]}]
  (if (nil? ensure?)
    (env/env-true? "DB_BOOTSTRAP")
    (boolean ensure?)))

(defmethod ig/init-key :core-service.app.config.databases/run-migrations
  [_ {:keys [run?]}]
  (if (nil? run?)
    (env/env-true? "DB_MIGRATIONS")
    (boolean run?)))
