;; DEV namespace for development tools and utilities to be tested in isolation 
;; and should never be required by any other namespace.

(ns core-service.dev.dev
  (:require [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            ;; Ensure Integrant has loaded the init/halt methods for the key
            ;; :core-service.clients.sqlite/client before we call ig/init-key.
            [core-service.clients.sqlite]
            [core-service.clients.postgres]
            [core-service.clients.typesense]
            [core-service.clients.typesense.client :as tc]
            [core-service.clients.jetstream]
            [core-service.producers.jetstream]
            [core-service.consumers.jetstream]
            [core-service.messaging.codecs.edn :as edn]
            [core-service.producers.protocol :as producer]
            ;; Ensure Integrant has loaded init-key methods for DB components too.
            [core-service.databases.sqlite]
            [core-service.databases.postgres]
            [core-service.databases.sql.common]
            [core-service.databases.protocols.simple-sql :as sql]
            ;; Text search layer
            [core-service.text-search]
            [core-service.text-search.typesense]
            [core-service.text-search.protocol :as ts]))

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

(defn smoke-sql-protocol!
  "Exercises CRUD via the SQL protocol layer (not raw JDBC) using SQLite under the hood."
  ([] (smoke-sql-protocol! {}))
  ([{:keys [jdbc-url pool? pool]
     :or {jdbc-url "jdbc:sqlite::memory:"
          pool? false
          pool {:maximum-pool-size 3
                :minimum-idle 1
                :connection-timeout-ms 30000}}}]
   (let [client (ig/init-key :core-service.clients.sqlite/client
                             {:jdbc-url jdbc-url :pool? pool? :pool pool})
         sqlite-db (ig/init-key :core-service.databases.sqlite/db
                                {:sqlite-client client})
         common-db (ig/init-key :core-service.databases.sql/common
                                {:default-engine :sqlite
                                 :engines {:sqlite sqlite-db}
                                 ;; logger is optional for dev; CommonSqlDatabase guards nil
                                 :logger nil})
         ds (:datasource client)
         table :dev_sql_protocol_smoke]
     (try
       (with-open [conn (jdbc/get-connection ds)]
         ;; Admin / schema
         (sql/create-table! common-db table
                            {:conn conn
                             :columns {:id "INTEGER PRIMARY KEY"
                                       :username "TEXT NOT NULL"
                                       :email "TEXT NOT NULL"}})
         (sql/truncate-table! common-db table {:conn conn})

         ;; Insert
         (sql/insert! common-db {:username "alice" :email "alice@example.com"}
                      {:conn conn :table table})
         (sql/insert! common-db {:username "bob" :email "bob@example.com"}
                      {:conn conn :table table})

         ;; Select
         (let [rows (sql/select common-db {:conn conn :table table :order-by [[:id :asc]]})]
           (println "SQL protocol select:" {:jdbc-url jdbc-url :rows rows}))

         ;; Select with Where clause
         (let [rows (sql/select common-db {:conn conn :table table :where {:username "bob"}})]
           (println "SQL protocol select with where clause:" {:jdbc-url jdbc-url :rows rows}))

        ;; select with more conditions, select where username is longer than 3 characters
        (let [rows (sql/select common-db {:conn conn :table table :where [:> [:length :username] 3]})]
           (println "SQL protocol select with where username is longer than 3 characters:" {:jdbc-url jdbc-url :rows rows}))

         ;; Update + select
         (sql/update! common-db {:email "bob@new.com"}
                      {:conn conn :table table :where {:username "bob"}})
         (let [bob (sql/select common-db {:conn conn :table table :where {:username "bob"}})]
           (println "SQL protocol after update:" bob))

         ;; Delete + select
         (sql/delete! common-db {:conn conn :table table :where {:username "alice"}})
         (let [rows (sql/select common-db {:conn conn :table table :order-by [[:id :asc]]})]
           (println "SQL protocol after delete:" rows)
           rows))
       (finally
         (ig/halt-key! :core-service.clients.sqlite/client client))))))

(run-smoke-tests!)
(smoke-sql-protocol!)

(defn smoke-postgres-client!
  "Verifies the Postgres client can connect and do basic writes/reads.

  Defaults match docker-compose.yaml:
  - jdbc-url: jdbc:postgresql://localhost:5432/core-service
  - username/password: postgres/postgres"
  ([] (smoke-postgres-client! {}))
  ([{:keys [jdbc-url username password pool? pool]
     :or {jdbc-url "jdbc:postgresql://localhost:5432/core-service"
          username "postgres"
          password "postgres"
          pool? false
          pool {:maximum-pool-size 3
                :minimum-idle 1
                :connection-timeout-ms 30000}}}]
   (let [client (ig/init-key :core-service.clients.postgres/client
                             {:jdbc-url jdbc-url
                              :username username
                              :password password
                              :pool? pool?
                              :pool pool})
         ds (:datasource client)]
     (try
       (with-open [conn (jdbc/get-connection ds)]
         (jdbc/execute! conn ["SELECT 1 AS ok"] {:builder-fn rs/as-unqualified-lower-maps})
         (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS dev_pg_smoke (id SERIAL PRIMARY KEY, value TEXT NOT NULL)"])
         (jdbc/execute! conn ["TRUNCATE TABLE dev_pg_smoke"])
         (jdbc/execute! conn ["INSERT INTO dev_pg_smoke (value) VALUES (?)" "hello"])
         (jdbc/execute! conn ["INSERT INTO dev_pg_smoke (value) VALUES (?)" "world"])
         (let [rows (jdbc/execute! conn
                                   ["SELECT id, value FROM dev_pg_smoke ORDER BY id"]
                                   {:builder-fn rs/as-unqualified-lower-maps})]
           (println "Postgres client smoke test OK:" {:jdbc-url jdbc-url :pool? pool? :rows rows})
           rows))
       (finally
         (ig/halt-key! :core-service.clients.postgres/client client))))))

(defn smoke-postgres-protocol!
  "Exercises CRUD via the SQL protocol layer against Postgres using the SQL common delegator."
  ([] (smoke-postgres-protocol! {}))
  ([{:keys [jdbc-url username password pool? pool]
     :or {jdbc-url "jdbc:postgresql://localhost:5432/core-service"
          username "postgres"
          password "postgres"
          pool? false
          pool {:maximum-pool-size 3
                :minimum-idle 1
                :connection-timeout-ms 30000}}}]
   (let [pg-client (ig/init-key :core-service.clients.postgres/client
                               {:jdbc-url jdbc-url
                                :username username
                                :password password
                                :pool? pool?
                                :pool pool})
         pg-db (ig/init-key :core-service.databases.postgres/db
                            {:postgres-client pg-client})
         common-db (ig/init-key :core-service.databases.sql/common
                                {:default-engine :postgres
                                 :engines {:postgres pg-db}
                                 :logger nil})
         ds (:datasource pg-client)
         table :dev_pg_protocol_smoke]
     (try
       (with-open [conn (jdbc/get-connection ds)]
         ;; Admin / schema
         (sql/create-table! common-db table
                            {:conn conn
                             :columns {:id "SERIAL PRIMARY KEY"
                                       :username "TEXT NOT NULL"
                                       :email "TEXT NOT NULL"}})
         (sql/truncate-table! common-db table {:conn conn})

         ;; Insert
         (sql/insert! common-db {:username "alice" :email "alice@example.com"}
                      {:conn conn :table table})
         (sql/insert! common-db {:username "bob" :email "bob@example.com"}
                      {:conn conn :table table})

         ;; Select + where + DSL
         (let [rows (sql/select common-db {:conn conn :table table :order-by [[:id :asc]]})]
           (println "Postgres protocol select:" {:rows rows}))
         (let [rows (sql/select common-db {:conn conn :table table :where {:username "bob"}})]
           (println "Postgres protocol select where bob:" {:rows rows}))
         (let [rows (sql/select common-db {:conn conn :table table :where [:> [:length :username] 3]})]
           (println "Postgres protocol select where length(username)>3:" {:rows rows}))

         ;; Update + select
         (sql/update! common-db {:email "bob@new.com"}
                      {:conn conn :table table :where {:username "bob"}})
         (let [bob (sql/select common-db {:conn conn :table table :where {:username "bob"}})]
           (println "Postgres protocol after update:" bob))

         ;; Delete + select
         (sql/delete! common-db {:conn conn :table table :where {:username "alice"}})
         (let [rows (sql/select common-db {:conn conn :table table :order-by [[:id :asc]]})]
           (println "Postgres protocol after delete:" rows)
           rows))
       (finally
         (ig/halt-key! :core-service.clients.postgres/client pg-client))))))

(defn smoke-typesense-client!
  "Verifies Typesense connectivity and basic indexing/search via the low-level client.

  Defaults match docker-compose.yaml:
  - endpoint: http://localhost:8108
  - api-key: typesense"
  ([] (smoke-typesense-client! {}))
  ([{:keys [endpoint api-key]
     :or {endpoint "http://localhost:8108"
          api-key "typesense"}}]
   (let [client (ig/init-key :core-service.clients.typesense/client
                             {:endpoint endpoint :api-key api-key})
         collection "dev_typesense_smoke"
         schema {:name collection
                 ;; Typesense reserves `id` for document ids, so we can't use it as a sortable field.
                 :fields [{:name "sort_id" :type "int32"}
                          {:name "username" :type "string"}
                          {:name "email" :type "string"}]
                 :default_sorting_field "sort_id"}]
     ;; 1) Prove connectivity/auth
     (let [resp (tc/get! client "/collections" {})]
       (when-not (<= 200 (:status resp) 299)
         (throw (ex-info "Typesense list collections failed" {:resp resp}))))

     ;; 2) Create collection (idempotent-ish)
     (let [resp (tc/post! client "/collections" {:body schema})]
       (when-not (or (<= 200 (:status resp) 299) (= 409 (:status resp)))
         (throw (ex-info "Typesense create collection failed" {:resp resp}))))

     ;; 3) Upsert docs
     (doseq [doc [{:id "1" :sort_id 1 :username "alice" :email "alice@example.com"}
                  {:id "2" :sort_id 2 :username "bob" :email "bob@example.com"}]]
       (let [resp (tc/post!
                    client
                    (str "/collections/" collection "/documents")
                    {:query {:action "upsert"}
                     :body doc})]
         (when-not (<= 200 (:status resp) 299)
           (throw (ex-info "Typesense upsert failed" {:doc doc :resp resp})))))

     ;; 4) Search
     (let [resp (tc/get!
                  client
                  (str "/collections/" collection "/documents/search")
                  {:query {:q "ali"
                           :query_by "username"}})]
       (when-not (<= 200 (:status resp) 299)
         (throw (ex-info "Typesense search failed" {:resp resp})))
       (println "Typesense client smoke test OK:" {:endpoint endpoint
                                                   :collection collection
                                                   :search-body (:body resp)})
       resp))))

(defn smoke-typesense-protocol!
  "Exercises the protocol/component layer for text search (not raw client calls)."
  ([] (smoke-typesense-protocol! {}))
  ([{:keys [endpoint api-key]
     :or {endpoint "http://localhost:8108"
          api-key "typesense"}}]
   (let [typesense-client (ig/init-key :core-service.clients.typesense/client
                                       {:endpoint endpoint :api-key api-key})
         engine (ig/init-key :core-service.text-search.typesense/engine
                             {:typesense-client typesense-client})
         common (ig/init-key :core-service.text-search/common
                             {:default-engine :typesense
                              :engines {:typesense engine}
                              :logger nil})
         collection "dev_typesense_protocol_smoke"
         schema {:name collection
                 :fields [{:name "sort_id" :type "int32"}
                          {:name "username" :type "string"}
                          {:name "email" :type "string"}]
                 :default_sorting_field "sort_id"}]
     ;; Admin
     (ts/create-collection! common collection schema {})
     ;; Docs
     (ts/upsert-document! common collection "1" {:sort_id 1 :username "alice" :email "alice@example.com"} {})
     (ts/upsert-document! common collection "2" {:sort_id 2 :username "bob" :email "bob@example.com"} {})
     ;; Query
     (let [resp (ts/search common collection {:q "ali" :query_by "username"} {})]
       (println "Typesense protocol smoke OK:" {:collection collection :search-body (:body resp)})
       resp))))

(defn smoke-jetstream!
  "Publishes a message to JetStream and verifies it is consumed via the JetStream runtime."
  ([] (smoke-jetstream! {}))
  ([{:keys [uri]
     :or {uri "nats://localhost:4222"}}]
   (let [received (promise)
         ;; Minimal routing config for dev smoke test.
         routing {:defaults {:source :jetstream}
                  :topics {:jetstream-test {:source :jetstream
                                            :subject "core.jetstream_test"
                                            :stream "core_jetstream_test"
                                            :durable "dev_jetstream_test"}}
                  :subscriptions {:jetstream-test {:source :jetstream
                                                   :topic :jetstream-test
                                                   :handler (fn [envelope]
                                                              (deliver received envelope))
                                                   :options {:pull-batch 1
                                                             :expires-ms 500}}}}
         codec (edn/->EdnCodec)
         jetstream (ig/init-key :core-service.clients.jetstream/client {:uri uri})
         producer (ig/init-key :core-service.producers.jetstream/producer
                               {:jetstream jetstream :routing routing :codec codec :logger nil})
         runtime (ig/init-key :core-service.consumers.jetstream/runtime
                              {:jetstream jetstream :routing routing :codec codec :dead-letter nil :logger nil})]
     (try
       (let [ack (producer/produce! producer
                                                          {:hello "jetstream"}
                                                          {:topic :jetstream-test})]
         (println "JetStream produced:" ack))
       (let [env (deref received 3000 ::timeout)]
         (when (= env ::timeout)
           (throw (ex-info "JetStream smoke test timed out waiting for consumption" {})))
         (println "JetStream consumed envelope:" env)
         env)
       (finally
         (ig/halt-key! :core-service.consumers.jetstream/runtime runtime)
         (ig/halt-key! :core-service.clients.jetstream/client jetstream))))))

;(smoke-jetstream!)