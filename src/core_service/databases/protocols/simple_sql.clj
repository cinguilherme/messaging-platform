(ns core-service.databases.protocols.simple-sql)

(defprotocol SimpleSQLProtocol
  (insert! [_ data opts]
    "Inserts `data` (a Clojure map) into an SQL table.

     Expected opts:
     - :table (required) keyword/string
     - :returning (optional) vector of columns to return (when supported by engine)")
  (select [_ opts]
    "Selects rows from an SQL table.

     Expected opts:
     - :table (required) keyword/string
     - :where (optional) map of equality predicates
     - :columns (optional) vector of columns, default: *
     - :limit / :offset (optional)
     - :order-by (optional) vector of columns or [col :asc|:desc]")
  (update! [_ data opts]
    "Updates rows in an SQL table using `data` (a Clojure map).

     Expected opts:
     - :table (required)
     - :where (required) map of equality predicates
     - :returning (optional) vector of columns to return (when supported by engine)")
  (delete! [_ opts]
    "Deletes rows from an SQL table.

     Expected opts:
     - :table (required)
     - :where (required) map of equality predicates")
  (execute! [_ sqlvec opts]
    "Escape hatch for engine-specific SQL execution.

     - `sqlvec` is a standard JDBC vector: [\"... ? ...\" param1 param2 ...]
     - `opts` is engine-specific and may include e.g. builder fns, result shape, etc.

     Implementations MUST throw (ex-info) with a consistent map when unsupported,
     rather than silently NO-OP." ))

(defprotocol SimpleSQLAdminProtocol
  (create-database! [_ database-name opts]
    "Creates a database. Implementations may throw ::not-supported.")
  (drop-database! [_ database-name opts]
    "Drops a database. Implementations may throw ::not-supported.")
  (create-table! [_ table-name opts]
    "Creates a table. `opts` may include engine-specific schema info or :ddl.")
  (drop-table! [_ table-name opts]
    "Drops a table. `opts` may include engine-specific options (e.g. :if-exists?).")
  (alter-table! [_ table-name opts]
    "Alters a table. Implementations may throw ::not-supported.")
  (rename-table! [_ table-name new-table-name opts]
    "Renames a table. Implementations may throw ::not-supported.")
  (truncate-table! [_ table-name opts]
    "Truncates a table. Implementations may throw ::not-supported."))