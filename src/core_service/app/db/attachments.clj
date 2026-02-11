(ns core-service.app.db.attachments
  (:require [clojure.string :as str]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [next.jdbc.result-set :as rs]))

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
  (let [attachment-ids (->> attachment-ids (remove nil?) distinct vec)]
    (when (seq attachment-ids)
      (let [placeholders (str/join "," (repeat (count attachment-ids) "?"))
            query (str "UPDATE attachments "
                       "SET referenced_at = COALESCE(referenced_at, now()) "
                       "WHERE attachment_id IN (" placeholders ")")]
        (sql/execute! db (into [query] attachment-ids) {})))))

(defn list-expired-attachments
  [db {:keys [cutoff limit]}]
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
  (sql/delete! db {:table :attachments
                   :where {:attachment_id attachment-id}}))
