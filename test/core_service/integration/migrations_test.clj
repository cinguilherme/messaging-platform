(ns core-service.integration.migrations-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]
            [next.jdbc.result-set :as rs]
            [core-service.app.config.databases]
            [core-service.app.db.migrations]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.postgres]
            [d-core.core.databases.postgres]
            [d-core.core.databases.sql.common]))

(defn- table-exists?
  [db table]
  (let [row (first (sql/execute! db
                                 ["SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=?) AS exists"
                                  (name table)]
                                 {:builder-fn rs/as-unqualified-lower-maps}))]
    (boolean (:exists row))))

(deftest migrations-create-tables
  (try
    (let [{:keys [db client]} (helpers/init-db)]
      (try
        (ig/init-key :core-service.app.db/migrations
                     {:db db
                      :path "resources/db/migrations"
                      :run? true})
        (testing "tables exist"
          (doseq [t [:conversations :memberships :segment_index]]
            (is (table-exists? db t) (str "missing table " t))))
        (testing "schema_migrations includes core migrations"
          (let [rows (sql/select db {:table :schema_migrations
                                     :columns [:id]
                                     :order-by [[:id :asc]]})
                applied (set (map :id rows))
                expected #{"001_create_conversations.sql"
                           "002_create_memberships.sql"
                           "003_create_segment_index.sql"
                           "004_create_indexes.sql"}]
            (is (set/subset? expected applied)
                (str "missing migrations: " (set/difference expected applied)))))
        (finally
          (ig/halt-key! :d-core.core.clients.postgres/client client))))
    (catch Exception ex
      (is false (str "Postgres not reachable or migrations failed: " (.getMessage ex))))))
