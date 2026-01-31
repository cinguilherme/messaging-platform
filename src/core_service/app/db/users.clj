(ns core-service.app.db.users
  (:require [clojure.string :as str]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [next.jdbc.result-set :as rs]))

(defn- normalize-id
  [value]
  (when value
    (try
      (if (instance? java.util.UUID value)
        value
        (java.util.UUID/fromString (str value)))
      (catch Exception _
        nil))))

(defn- normalize-text
  [value]
  (when (string? value)
    (let [v (str/trim value)]
      (when-not (str/blank? v)
        v))))

(defn- normalize-email
  [value]
  (some-> value normalize-text str/lower-case))

(defn upsert-user-profile!
  [db {:keys [user-id username first-name last-name avatar-url email enabled]}]
  (let [user-id (normalize-id user-id)
        username (normalize-text username)
        first-name (normalize-text first-name)
        last-name (normalize-text last-name)
        avatar-url (normalize-text avatar-url)
        email (normalize-email email)]
    (when user-id
      (sql/execute! db
                    [(str "INSERT INTO user_profiles "
                          "(user_id, username, first_name, last_name, avatar_url, email, enabled, updated_at) "
                          "VALUES (?, ?, ?, ?, ?, ?, ?, now()) "
                          "ON CONFLICT (user_id) DO UPDATE SET "
                          "username = EXCLUDED.username, "
                          "first_name = EXCLUDED.first_name, "
                          "last_name = EXCLUDED.last_name, "
                          "avatar_url = EXCLUDED.avatar_url, "
                          "email = EXCLUDED.email, "
                          "enabled = EXCLUDED.enabled, "
                          "updated_at = now()")
                     user-id
                     username
                     first-name
                     last-name
                     avatar-url
                     email
                     enabled]
                    {}))))

(defn fetch-user-profiles
  [db {:keys [user-ids]}]
  (let [user-ids (->> user-ids (map normalize-id) (remove nil?) vec)]
    (if-not (seq user-ids)
      []
      (let [placeholders (str/join "," (repeat (count user-ids) "?"))
            query (str "SELECT user_id, username, first_name, last_name, avatar_url, email, enabled "
                       "FROM user_profiles "
                       "WHERE user_id IN (" placeholders ")")]
        (sql/execute! db (into [query] user-ids)
                      {:builder-fn rs/as-unqualified-lower-maps})))))

(defn fetch-user-profile
  [db {:keys [user-id]}]
  (first (fetch-user-profiles db {:user-ids [user-id]})))

(defn fetch-user-profile-by-username
  [db {:keys [username]}]
  (when (and db username)
    (first
     (sql/execute! db
                   [(str "SELECT user_id, username, first_name, last_name, avatar_url, email, enabled "
                         "FROM user_profiles WHERE username = ? LIMIT 1")
                    username]
                   {:builder-fn rs/as-unqualified-lower-maps}))))
