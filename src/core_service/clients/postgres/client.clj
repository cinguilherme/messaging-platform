(ns core-service.clients.postgres.client
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (org.postgresql.ds PGSimpleDataSource)
           (javax.sql DataSource)))

(defrecord PostgresClient [^DataSource datasource jdbc-url pooled?]
  Object
  (toString [_] (str "#PostgresClient{:jdbc-url " (pr-str jdbc-url) ", :pooled? " (boolean pooled?) "}")))

(defn- pg-datasource
  ^DataSource
  [{:keys [jdbc-url username password]}]
  (let [^PGSimpleDataSource ds (PGSimpleDataSource.)]
    (.setURL ds jdbc-url)
    (when (some? username)
      (.setUser ds (str username)))
    (when (some? password)
      (.setPassword ds (str password)))
    ds))

(defn- hikari-datasource
  ^HikariDataSource
  [{:keys [jdbc-url username password pool]}]
  (let [{:keys [pool-name
                maximum-pool-size
                minimum-idle
                connection-timeout-ms
                idle-timeout-ms
                max-lifetime-ms]} (or pool {})
        ^HikariConfig cfg (HikariConfig.)]
    (.setJdbcUrl cfg jdbc-url)
    (when username (.setUsername cfg (str username)))
    (when password (.setPassword cfg (str password)))
    (when pool-name (.setPoolName cfg (str pool-name)))
    (when maximum-pool-size (.setMaximumPoolSize cfg (int maximum-pool-size)))
    (when minimum-idle (.setMinimumIdle cfg (int minimum-idle)))
    (when connection-timeout-ms (.setConnectionTimeout cfg (long connection-timeout-ms)))
    (when idle-timeout-ms (.setIdleTimeout cfg (long idle-timeout-ms)))
    (when max-lifetime-ms (.setMaxLifetime cfg (long max-lifetime-ms)))
    (HikariDataSource. cfg)))

(defn make-client
  "Builds a PostgresClient with either an unpooled PGSimpleDataSource or a pooled Hikari DataSource."
  [{:keys [jdbc-url username password pool? pool]
    :or {jdbc-url "jdbc:postgresql://localhost:5432/core-service"
         username "postgres"
         password "postgres"
         pool? false
         pool {}}}]
  (let [ds (if pool?
             (hikari-datasource {:jdbc-url jdbc-url :username username :password password :pool pool})
             (pg-datasource {:jdbc-url jdbc-url :username username :password password}))]
    (->PostgresClient ds jdbc-url pool?)))

(defn close!
  "Closes resources held by the client (only needed for pooled/Hikari datasources)."
  [pg-client]
  (when-let [ds (:datasource pg-client)]
    (when (instance? HikariDataSource ds)
      (.close ^HikariDataSource ds))))