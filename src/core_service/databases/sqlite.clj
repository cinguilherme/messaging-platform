(ns core-service.databases.sqlite
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [core-service.databases.protocols.simple-sql :as p]))

(defn- not-supported
  [op details]
  (throw (ex-info "SQL operation not supported by engine"
                  {:type ::not-supported
                   :engine :sqlite
                   :op op
                   :details details})))

(defn- ident
  "Conservative identifier sanitizer to avoid SQL injection via table/column names.
   Accepts keywords or strings matching ^[A-Za-z_][A-Za-z0-9_]*$."
  [x {:keys [kind]}]
  (let [s (cond
            (keyword? x) (name x)
            (string? x) x
            :else (throw (ex-info "Identifier must be keyword or string" {:kind kind :value x})))]
    (when-not (re-matches #"[A-Za-z_][A-Za-z0-9_]*" s)
      (throw (ex-info "Unsafe identifier" {:kind kind :value s})))
    (str "\"" s "\"")))

(defn- sql-target
  "Uses an existing connection/tx if provided in opts (critical for sqlite :memory:),
   otherwise uses the DataSource."
  [datasource opts]
  (or (:conn opts) (:tx opts) datasource))

(defn- where->sql
  "Turns a simple equality where-map into [\"col1 = ? AND col2 = ?\" [v1 v2]]."
  [where]
  (when (some? where)
    (when-not (map? where)
      (throw (ex-info ":where must be a map" {:where where})))
    (let [ks (keys where)
          clauses (map (fn [k] (str (ident k {:kind :column}) " = ?")) ks)
          params (mapv where ks)]
      [(str/join " AND " clauses) params])))

(defn- order-by->sql
  [order-by]
  (when (some? order-by)
    (when-not (sequential? order-by)
      (throw (ex-info ":order-by must be sequential" {:order-by order-by})))
    (let [parts
          (map (fn [x]
                 (cond
                   (or (keyword? x) (string? x))
                   (ident x {:kind :column})

                   (and (vector? x) (= 2 (count x)))
                   (let [[col dir] x
                         dir' (or dir :asc)]
                     (when-not (#{:asc :desc} dir')
                       (throw (ex-info "Order direction must be :asc or :desc" {:dir dir'})))
                     (str (ident col {:kind :column}) " " (str/upper-case (name dir'))))

                   :else
                   (throw (ex-info "Invalid :order-by entry" {:entry x}))))
               order-by)]
      (str/join ", " parts))))

(defrecord SqliteDatabase [datasource]
  p/SimpleSQLProtocol
  (execute! [_ sqlvec opts]
    (jdbc/execute! (sql-target datasource opts) sqlvec (or opts {})))

  (insert! [_ data {:keys [table returning] :as opts}]
    (when-not (map? data)
      (throw (ex-info "insert! expects `data` as a map" {:data data})))
    (when-not table
      (throw (ex-info "insert! requires :table in opts" {:opts opts})))
    (let [cols (keys data)
          vals (mapv data cols)
          cols-sql (str/join ", " (map #(ident % {:kind :column}) cols))
          placeholders (str/join ", " (repeat (count cols) "?"))
          returning-sql (when (seq returning)
                          (str " RETURNING " (str/join ", " (map #(ident % {:kind :column}) returning))))
          sql (str "INSERT INTO " (ident table {:kind :table})
                   " (" cols-sql ") VALUES (" placeholders ")"
                   (or returning-sql ""))]
      (jdbc/execute! (sql-target datasource opts) (into [sql] vals) (or opts {}))))

  (select [_ {:keys [table columns where limit offset order-by] :as opts}]
    (when-not table
      (throw (ex-info "select requires :table in opts" {:opts opts})))
    (let [[where-sql where-params] (or (where->sql where) [nil []])
          cols-sql (if (seq columns)
                     (str/join ", " (map #(ident % {:kind :column}) columns))
                     "*")
          order-by-sql (order-by->sql order-by)
          sql (str "SELECT " cols-sql " FROM " (ident table {:kind :table})
                   (when where-sql (str " WHERE " where-sql))
                   (when order-by-sql (str " ORDER BY " order-by-sql))
                   (when (some? limit) (str " LIMIT " (long limit)))
                   (when (some? offset) (str " OFFSET " (long offset))))
          base-opts (merge {:builder-fn rs/as-unqualified-lower-maps} (or opts {}))]
      (jdbc/execute! (sql-target datasource opts) (into [sql] where-params) base-opts)))

  (update! [_ data {:keys [table where returning] :as opts}]
    (when-not (map? data)
      (throw (ex-info "update! expects `data` as a map" {:data data})))
    (when-not table
      (throw (ex-info "update! requires :table in opts" {:opts opts})))
    (when-not (and where (map? where) (seq where))
      (throw (ex-info "update! requires non-empty :where map in opts" {:opts opts})))
    (let [cols (keys data)
          set-sql (str/join ", " (map (fn [k] (str (ident k {:kind :column}) " = ?")) cols))
          set-params (mapv data cols)
          [where-sql where-params] (where->sql where)
          returning-sql (when (seq returning)
                          (str " RETURNING " (str/join ", " (map #(ident % {:kind :column}) returning))))
          sql (str "UPDATE " (ident table {:kind :table})
                   " SET " set-sql
                   " WHERE " where-sql
                   (or returning-sql ""))]
      (jdbc/execute! (sql-target datasource opts)
                    (into [sql] (into set-params where-params))
                    (or opts {}))))

  (delete! [_ {:keys [table where] :as opts}]
    (when-not table
      (throw (ex-info "delete! requires :table in opts" {:opts opts})))
    (when-not (and where (map? where) (seq where))
      (throw (ex-info "delete! requires non-empty :where map in opts" {:opts opts})))
    (let [[where-sql where-params] (where->sql where)
          sql (str "DELETE FROM " (ident table {:kind :table}) " WHERE " where-sql)]
      (jdbc/execute! (sql-target datasource opts) (into [sql] where-params) (or opts {}))))

  p/SimpleSQLAdminProtocol
  (create-database! [_ database-name opts]
    (not-supported :create-database! {:database-name database-name :opts opts}))

  (drop-database! [_ database-name opts]
    (not-supported :drop-database! {:database-name database-name :opts opts}))

  (create-table! [_ table-name {:keys [ddl columns if-not-exists?] :as opts}]
    (cond
      (string? ddl)
      (jdbc/execute! (sql-target datasource opts) [ddl] (or opts {}))

      (map? columns)
      (let [if-not-exists? (if (nil? if-not-exists?) true (boolean if-not-exists?))
            cols-sql (str/join ", "
                               (map (fn [[col type]]
                                      (when-not (string? type)
                                        (throw (ex-info "Column type must be string" {:column col :type type})))
                                      (str (ident col {:kind :column}) " " type))
                                    columns))
            sql (str "CREATE TABLE "
                     (when if-not-exists? "IF NOT EXISTS ")
                     (ident table-name {:kind :table})
                     " (" cols-sql ")")]
        (jdbc/execute! (sql-target datasource opts) [sql] (or opts {})))

      :else
      (throw (ex-info "create-table! requires either :ddl (string) or :columns (map) in opts"
                      {:table-name table-name :opts opts}))))

  (drop-table! [_ table-name {:keys [if-exists?] :as opts}]
    (let [if-exists? (if (nil? if-exists?) true (boolean if-exists?))
          sql (str "DROP TABLE " (when if-exists? "IF EXISTS ") (ident table-name {:kind :table}))]
      (jdbc/execute! (sql-target datasource opts) [sql] (or opts {}))))

  (alter-table! [_ table-name opts]
    (not-supported :alter-table! {:table-name table-name :opts opts}))

  (rename-table! [_ table-name new-table-name opts]
    (let [sql (str "ALTER TABLE " (ident table-name {:kind :table})
                   " RENAME TO " (ident new-table-name {:kind :table}))]
      (jdbc/execute! (sql-target datasource opts) [sql] (or opts {}))))

  (truncate-table! [_ table-name opts]
    ;; SQLite doesn't have TRUNCATE; use DELETE.
    (let [sql (str "DELETE FROM " (ident table-name {:kind :table}))]
      (jdbc/execute! (sql-target datasource opts) [sql] (or opts {})))))

(defmethod ig/init-key :core-service.databases.sqlite/db
  [_ {:keys [sqlite-client] :as opts}]
  (when-not sqlite-client
    (throw (ex-info "SQLite DB component requires :sqlite-client" {:opts opts})))
  (->SqliteDatabase (:datasource sqlite-client)))

