(ns core-service.core.clients.sqlite.client
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (org.sqlite SQLiteDataSource)
           (javax.sql DataSource)))

(defrecord SqliteClient [^DataSource datasource jdbc-url pooled?]
  Object
  (toString [_] (str "#SqliteClient{:jdbc-url " (pr-str jdbc-url) ", :pooled? " (boolean pooled?) "}")))

(defn- sqlite-datasource
  ^DataSource
  [^String jdbc-url]
  (doto (SQLiteDataSource.)
    (.setUrl jdbc-url)))

(defn- hikari-datasource
  ^HikariDataSource
  [^String jdbc-url {:keys [pool-name
                            maximum-pool-size
                            minimum-idle
                            connection-timeout-ms
                            idle-timeout-ms
                            max-lifetime-ms]}]
  (let [^HikariConfig cfg (HikariConfig.)]
    (.setJdbcUrl cfg jdbc-url)
    (when pool-name
      (.setPoolName cfg (str pool-name)))
    (when maximum-pool-size
      (.setMaximumPoolSize cfg (int maximum-pool-size)))
    (when minimum-idle
      (.setMinimumIdle cfg (int minimum-idle)))
    (when connection-timeout-ms
      (.setConnectionTimeout cfg (long connection-timeout-ms)))
    (when idle-timeout-ms
      (.setIdleTimeout cfg (long idle-timeout-ms)))
    (when max-lifetime-ms
      (.setMaxLifetime cfg (long max-lifetime-ms)))
    (HikariDataSource. cfg)))

(defn make-client
  "Builds a SqliteClient with either an unpooled SQLite DataSource or a pooled Hikari DataSource."
  [{:keys [jdbc-url pool? pool]
    :or {jdbc-url "jdbc:sqlite::memory:"
         pool? false
         pool {}}}]
  (let [ds (if pool?
             (hikari-datasource jdbc-url pool)
             (sqlite-datasource jdbc-url))]
    (->SqliteClient ds jdbc-url pool?)))

(defn close!
  "Closes resources held by the client (only needed for pooled/Hikari datasources)."
  [sqlite-client]
  (when-let [ds (:datasource sqlite-client)]
    ;; Only pooled datasources need explicit close (SQLiteDataSource doesn't).
    (when (instance? HikariDataSource ds)
      (.close ^HikariDataSource ds))))