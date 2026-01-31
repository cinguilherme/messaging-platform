(ns core-service.app.db.messaging-tables
  (:require [clojure.string :as str]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [duct.logger :as logger]
            [integrant.core :as ig]))

(def default-tables
  {:conversations
   {:ddl (str "CREATE TABLE IF NOT EXISTS conversations ("
              "id UUID PRIMARY KEY, "
              "tenant_id TEXT NOT NULL, "
              "type TEXT NOT NULL, "
              "title TEXT NULL, "
              "created_at TIMESTAMPTZ NOT NULL DEFAULT now()"
              ")")}

   :memberships
   {:ddl (str "CREATE TABLE IF NOT EXISTS memberships ("
              "conversation_id UUID NOT NULL, "
              "user_id UUID NOT NULL, "
              "role TEXT NOT NULL, "
              "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
              "PRIMARY KEY (conversation_id, user_id)"
              ")")
    :indexes ["CREATE INDEX IF NOT EXISTS memberships_user_id_idx ON memberships (user_id)"]}

   :segment_index
   {:ddl (str "CREATE TABLE IF NOT EXISTS segment_index ("
              "conversation_id UUID NOT NULL, "
              "seq_start BIGINT NOT NULL, "
              "seq_end BIGINT NOT NULL, "
              "object_key TEXT NOT NULL, "
              "byte_size BIGINT NOT NULL, "
              "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
              "PRIMARY KEY (conversation_id, seq_start)"
              ")")
    :indexes ["CREATE INDEX IF NOT EXISTS segment_index_conv_end_idx ON segment_index (conversation_id, seq_end)"]}

   :user_profiles
   {:ddl (str "CREATE TABLE IF NOT EXISTS user_profiles ("
              "user_id UUID PRIMARY KEY, "
              "username TEXT NULL, "
              "first_name TEXT NULL, "
              "last_name TEXT NULL, "
              "avatar_url TEXT NULL, "
              "email TEXT NULL, "
              "enabled BOOLEAN NULL, "
              "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
              "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
              ")")
    :indexes ["CREATE INDEX IF NOT EXISTS user_profiles_username_idx ON user_profiles (username)"
              "CREATE INDEX IF NOT EXISTS user_profiles_email_idx ON user_profiles (email)"]}})

(defn- apply-ddl!
  [db ddl logger]
  (when (seq ddl)
    (when logger
      (logger/log logger :info ::ddl {:sql (subs ddl 0 (min 80 (count ddl)))}))
    (sql/execute! db [ddl] {})))

(defn- apply-indexes!
  [db indexes logger]
  (doseq [ddl indexes]
    (when logger
      (logger/log logger :info ::index {:sql (subs ddl 0 (min 80 (count ddl)))}))
    (sql/execute! db [ddl] {})))

(defmethod ig/init-key :core-service.app.db.messaging-tables/tables
  [_ {:keys [db tables ensure? logger]}]
  (let [tables (merge default-tables tables)
        ensure? (boolean ensure?)]
    (when ensure?
      (doseq [[_ {:keys [ddl indexes]}] tables]
        (apply-ddl! db ddl logger)
        (apply-indexes! db indexes logger)))
    {:tables tables}))
