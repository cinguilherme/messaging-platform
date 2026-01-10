;; DEV namespace for development tools and utilities to be tested in isolation 
;; and should never be required by any other namespace.

(ns core-service.dev.dev
  (:require [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            ;; Ensure Integrant has loaded the init/halt methods for the key
            ;; :core-service.clients.sqlite/client before we call ig/init-key.
            [core-service.clients.sqlite]))

(defn smoke-sqlite!
  "Creates a table, inserts rows, and reads them back.

  Options:
  - :jdbc-url (string)  default: in-memory
  - :pool?   (boolean) default: false
  - :pool    (map)     hikari options when :pool? true"
  ([] (smoke-sqlite! {}))
  ([{:keys [jdbc-url pool? pool]
     :or {jdbc-url "jdbc:sqlite::memory:"
          pool? false
          pool {:maximum-pool-size 3
                :minimum-idle 1
                :connection-timeout-ms 30000}}}]
   (let [client (ig/init-key :core-service.clients.sqlite/client
                             {:jdbc-url jdbc-url :pool? pool? :pool pool})
         ds (:datasource client)]
     (try
       ;; Important for SQLite in-memory: the DB is per-connection, so keep one conn
       ;; for the whole smoke test sequence.
       (with-open [conn (jdbc/get-connection ds)]
         (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS dev_smoke (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"])
         (jdbc/execute! conn ["DELETE FROM dev_smoke"])
         (jdbc/execute! conn ["INSERT INTO dev_smoke (value) VALUES (?)" "hello"])
         (jdbc/execute! conn ["INSERT INTO dev_smoke (value) VALUES (?)" "world"])
         (let [rows (jdbc/execute! conn
                                   ["SELECT id, value FROM dev_smoke ORDER BY id"]
                                   {:builder-fn rs/as-unqualified-lower-maps})]
           (println "SQLite smoke test OK:" {:jdbc-url jdbc-url :pool? pool? :rows rows})
           rows))
       (finally
         (ig/halt-key! :core-service.clients.sqlite/client client))))))

(defn run-smoke-tests!
  "Runs both in-memory and file-backed smoke tests."
  []
  (smoke-sqlite! {:jdbc-url "jdbc:sqlite::memory:"})
  (smoke-sqlite! {:jdbc-url "jdbc:sqlite:storage/dev-smoke.db"}))

(run-smoke-tests!)