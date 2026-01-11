(ns core-service.core.databases.postgres
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [core-service.core.databases.protocols.simple-sql :as p]))

(defn- not-supported
  [op details]
  (throw (ex-info "SQL operation not supported by engine"
                  {:type ::not-supported
                   :engine :postgres
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
  "Uses an existing connection/tx if provided in opts, otherwise uses the DataSource."
  [datasource opts]
  (or (:conn opts) (:tx opts) datasource))

(defn- expr->sql
  "Same where DSL as SQLite: returns [sql params]."
  [expr]
  (cond
    (vector? expr)
    (let [[op & xs] expr]
      (when-not (keyword? op)
        (throw (ex-info "Where expression must start with an operator keyword" {:expr expr})))
      (case op
        :and (let [parts (map expr->sql xs)
                   sql (str/join " AND " (map first parts))
                   args (vec (mapcat second parts))]
               [(str "(" sql ")") args])

        :or (let [parts (map expr->sql xs)
                  sql (str/join " OR " (map first parts))
                  args (vec (mapcat second parts))]
              [(str "(" sql ")") args])

        :not (let [[sql args] (expr->sql (first xs))]
               [(str "NOT (" sql ")") args])

        := (let [[lhs rhs] xs
                 [lhs-sql lhs-args] (expr->sql lhs)]
             (if (nil? rhs)
               [(str lhs-sql " IS NULL") (vec lhs-args)]
               [(str lhs-sql " = ?") (conj (vec lhs-args) rhs)]))

        :!= (let [[lhs rhs] xs
                  [lhs-sql lhs-args] (expr->sql lhs)]
              (if (nil? rhs)
                [(str lhs-sql " IS NOT NULL") (vec lhs-args)]
                [(str lhs-sql " <> ?") (conj (vec lhs-args) rhs)]))

        :> (let [[lhs rhs] xs
                 [lhs-sql lhs-args] (expr->sql lhs)]
             [(str lhs-sql " > ?") (conj (vec lhs-args) rhs)])

        :>= (let [[lhs rhs] xs
                  [lhs-sql lhs-args] (expr->sql lhs)]
              [(str lhs-sql " >= ?") (conj (vec lhs-args) rhs)])

        :< (let [[lhs rhs] xs
                 [lhs-sql lhs-args] (expr->sql lhs)]
             [(str lhs-sql " < ?") (conj (vec lhs-args) rhs)])

        :<= (let [[lhs rhs] xs
                  [lhs-sql lhs-args] (expr->sql lhs)]
              [(str lhs-sql " <= ?") (conj (vec lhs-args) rhs)])

        :like (let [[lhs pattern] xs
                    [lhs-sql lhs-args] (expr->sql lhs)]
                [(str lhs-sql " LIKE ?") (conj (vec lhs-args) pattern)])

        :in (let [[lhs coll] xs
                  coll (vec coll)
                  [lhs-sql lhs-args] (expr->sql lhs)]
              (when (empty? coll)
                (throw (ex-info ":in requires a non-empty collection" {:expr expr})))
              [(str lhs-sql " IN (" (str/join ", " (repeat (count coll) "?")) ")")
               (into (vec lhs-args) coll)])

        :length (let [[col] xs]
                  [(str "LENGTH(" (ident col {:kind :column}) ")") []])

        (throw (ex-info "Unknown where operator" {:op op :expr expr}))))

    (keyword? expr)
    [(ident expr {:kind :column}) []]

    (string? expr)
    [(ident expr {:kind :column}) []]

    :else
    (throw (ex-info "Invalid where expression" {:expr expr}))))

(defn- map-where->expr
  [m]
  (into
    [:and]
    (map (fn [[k v]]
           (cond
             (vector? v)
             (let [[op rhs] v]
               (case op
                 :> [:> k rhs]
                 :>= [:>= k rhs]
                 :< [:< k rhs]
                 :<= [:<= k rhs]
                 :!= [:!= k rhs]
                 :like [:like k rhs]
                 :in [:in k rhs]
                 := [:= k rhs]
                 (throw (ex-info "Unknown map :where operator" {:op op :entry [k v]}))))
             :else
             [:= k v])))
    m))

(defn- where->sql
  [where]
  (cond
    (nil? where) [nil []]
    (map? where) (expr->sql (map-where->expr where))
    (vector? where) (expr->sql where)
    :else (throw (ex-info ":where must be a map or vector expression" {:where where}))))

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

(defrecord PostgresDatabase [datasource]
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
    (let [[where-sql where-params] (where->sql where)
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

  (truncate-table! [_ table-name {:keys [restart-identity? cascade?] :as opts}]
    (let [sql (str "TRUNCATE TABLE " (ident table-name {:kind :table})
                   (when restart-identity? " RESTART IDENTITY")
                   (when cascade? " CASCADE"))]
      (jdbc/execute! (sql-target datasource opts) [sql] (or (dissoc opts :restart-identity? :cascade?) {})))))

(defmethod ig/init-key :core-service.core.databases.postgres/db
  [_ {:keys [postgres-client] :as opts}]
  (when-not postgres-client
    (throw (ex-info "Postgres DB component requires :postgres-client" {:opts opts})))
  (->PostgresDatabase (:datasource postgres-client)))

