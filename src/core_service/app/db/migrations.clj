(ns core-service.app.db.migrations
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [duct.logger :as logger]
            [integrant.core :as ig]))

(defn- list-migration-files
  [path]
  (let [dir (io/file path)]
    (when (.exists dir)
      (->> (file-seq dir)
           (filter #(.isFile ^java.io.File %))
           (filter #(str/ends-with? (.getName ^java.io.File %) ".sql"))
           (sort-by #(.getName ^java.io.File %))))))

(defn- split-sql
  [sql-text]
  (->> (str/split sql-text #";")
       (map str/trim)
       (remove str/blank?)))

(defn- ensure-schema-migrations!
  [db logger]
  (let [ddl (str "CREATE TABLE IF NOT EXISTS schema_migrations ("
                 "id TEXT PRIMARY KEY, "
                 "applied_at TIMESTAMPTZ NOT NULL DEFAULT now()"
                 ")")]
    (when logger
      (logger/log logger :info ::schema-migrations))
    (sql/execute! db [ddl] {})))

(defn- applied-migrations
  [db]
  (->> (sql/select db {:table :schema_migrations
                       :columns [:id]
                       :order-by [[:id :asc]]})
       (map :id)
       set))

(defn- apply-migration!
  [db {:keys [id path sql-text]} logger]
  (when logger
    (logger/log logger :info ::applying-migration {:id id :path path}))
  (doseq [stmt (split-sql sql-text)]
    (sql/execute! db [stmt] {}))
  (sql/insert! db {:id id} {:table :schema_migrations}))

(defmethod ig/init-key :core-service.app.db/migrations
  [_ {:keys [db path run? logger]}]
  (when run?
    (let [path (or path "resources/db/migrations")
          files (list-migration-files path)]
      (when-not files
        (throw (ex-info "Migrations path not found" {:path path})))
      (ensure-schema-migrations! db logger)
      (let [applied (applied-migrations db)]
        (doseq [f files]
          (let [id (.getName ^java.io.File f)]
            (when-not (contains? applied id)
              (apply-migration! db {:id id
                                    :path (.getAbsolutePath ^java.io.File f)
                                    :sql-text (slurp f)}
                                logger)))))))
  {:run? (boolean run?)})
