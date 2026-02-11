(ns core-service.app.db.attachments
  (:require [clojure.string :as str]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [next.jdbc.result-set :as rs]))

(def ^:private attachments-ddl
  (str "CREATE TABLE IF NOT EXISTS attachments ("
       "attachment_id UUID PRIMARY KEY, "
       "conversation_id UUID NOT NULL, "
       "uploader_id UUID NOT NULL, "
       "object_key TEXT NOT NULL UNIQUE, "
       "mime_type TEXT NOT NULL, "
       "size_bytes BIGINT NOT NULL, "
       "checksum TEXT NOT NULL, "
       "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
       "expires_at TIMESTAMPTZ NOT NULL, "
       "referenced_at TIMESTAMPTZ NULL"
       ")"))

(def ^:private attachments-indexes
  ["CREATE INDEX IF NOT EXISTS attachments_conversation_idx ON attachments (conversation_id)"
   "CREATE INDEX IF NOT EXISTS attachments_expires_idx ON attachments (expires_at)"])

(defonce ^:private attachments-table-ready? (atom false))

(defn- ensure-attachments-table!
  [db]
  (when-not @attachments-table-ready?
    (locking attachments-table-ready?
      (when-not @attachments-table-ready?
        (sql/execute! db [attachments-ddl] {})
        (doseq [ddl attachments-indexes]
          (sql/execute! db [ddl] {}))
        (reset! attachments-table-ready? true)))))

(defn- ->timestamp
  [value]
  (cond
    (instance? java.sql.Timestamp value) value
    (instance? java.time.Instant value) (java.sql.Timestamp/from value)
    (instance? java.util.Date value) (java.sql.Timestamp. (.getTime ^java.util.Date value))
    (number? value) (java.sql.Timestamp. (long value))
    :else nil))

(defn insert-attachment!
  [db {:keys [attachment-id conversation-id uploader-id object-key mime-type
              size-bytes checksum expires-at]}]
  (ensure-attachments-table! db)
  (sql/insert! db {:attachment_id attachment-id
                   :conversation_id conversation-id
                   :uploader_id uploader-id
                   :object_key object-key
                   :mime_type mime-type
                   :size_bytes (long size-bytes)
                   :checksum checksum
                   :expires_at (or (->timestamp expires-at)
                                   (java.sql.Timestamp.
                                    (+ (System/currentTimeMillis) 2592000000)))}
               {:table :attachments}))

(defn fetch-attachments-by-ids
  [db attachment-ids]
  (ensure-attachments-table! db)
  (let [attachment-ids (->> attachment-ids (remove nil?) distinct vec)]
    (if-not (seq attachment-ids)
      []
      (let [placeholders (str/join "," (repeat (count attachment-ids) "?"))
            query (str "SELECT attachment_id, conversation_id, uploader_id, object_key, "
                       "mime_type, size_bytes, checksum, created_at, expires_at, referenced_at "
                       "FROM attachments "
                       "WHERE attachment_id IN (" placeholders ")")]
        (sql/execute! db (into [query] attachment-ids)
                      {:builder-fn rs/as-unqualified-lower-maps})))))

(defn mark-referenced!
  [db attachment-ids]
  (ensure-attachments-table! db)
  (let [attachment-ids (->> attachment-ids (remove nil?) distinct vec)]
    (when (seq attachment-ids)
      (let [placeholders (str/join "," (repeat (count attachment-ids) "?"))
            query (str "UPDATE attachments "
                       "SET referenced_at = COALESCE(referenced_at, now()) "
                       "WHERE attachment_id IN (" placeholders ")")]
        (sql/execute! db (into [query] attachment-ids) {})))))

(defn list-expired-attachments
  [db {:keys [cutoff limit]}]
  (ensure-attachments-table! db)
  (let [limit (long (or limit 200))
        cutoff-ts (or (->timestamp cutoff)
                      (java.sql.Timestamp. (System/currentTimeMillis)))]
    (sql/execute! db
                  [(str "SELECT attachment_id, conversation_id, object_key, expires_at "
                        "FROM attachments "
                        "WHERE expires_at <= ? "
                        "ORDER BY expires_at ASC "
                        "LIMIT ?")
                   cutoff-ts limit]
                  {:builder-fn rs/as-unqualified-lower-maps})))

(defn delete-attachment!
  [db {:keys [attachment-id]}]
  (ensure-attachments-table! db)
  (sql/delete! db {:table :attachments
                   :where {:attachment_id attachment-id}}))
