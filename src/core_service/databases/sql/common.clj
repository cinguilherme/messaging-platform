(ns core-service.databases.sql.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.databases.protocols.simple-sql :as p]))

(defn- log*
  [maybe-logger level event data]
  (when maybe-logger
    (logger/log maybe-logger level event data)))

(defn- delegate
  [engines default-engine opts]
  (let [engine (or (:engine opts) default-engine)
        db (get engines engine)]
    (when-not db
      (throw (ex-info "Unknown SQL engine" {:engine engine :known (keys engines)})))
    [engine db]))

(defrecord CommonSqlDatabase [default-engine engines logger]
  p/SimpleSQLProtocol
  (insert! [_ data opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :debug ::insert! {:engine engine :table (:table opts)})
      (p/insert! db data opts)))
  (select [_ opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :debug ::select {:engine engine :table (:table opts)})
      (p/select db opts)))
  (update! [_ data opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :debug ::update! {:engine engine :table (:table opts)})
      (p/update! db data opts)))
  (delete! [_ opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :debug ::delete! {:engine engine :table (:table opts)})
      (p/delete! db opts)))
  (execute! [_ sqlvec opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :debug ::execute! {:engine engine})
      (p/execute! db sqlvec opts)))

  p/SimpleSQLAdminProtocol
  (create-database! [_ database-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::create-database! {:engine engine :database database-name})
      (p/create-database! db database-name opts)))
  (drop-database! [_ database-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::drop-database! {:engine engine :database database-name})
      (p/drop-database! db database-name opts)))
  (create-table! [_ table-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::create-table! {:engine engine :table table-name})
      (p/create-table! db table-name opts)))
  (drop-table! [_ table-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::drop-table! {:engine engine :table table-name})
      (p/drop-table! db table-name opts)))
  (alter-table! [_ table-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::alter-table! {:engine engine :table table-name})
      (p/alter-table! db table-name opts)))
  (rename-table! [_ table-name new-table-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::rename-table! {:engine engine :table table-name :new-table new-table-name})
      (p/rename-table! db table-name new-table-name opts)))
  (truncate-table! [_ table-name opts]
    (let [[engine db] (delegate engines default-engine opts)]
      (log* logger :info ::truncate-table! {:engine engine :table table-name})
      (p/truncate-table! db table-name opts))))

(defmethod ig/init-key :core-service.databases.sql/common
  [_ {:keys [default-engine engines logger]
      :or {default-engine :sqlite}}]
  (->CommonSqlDatabase default-engine engines logger))

