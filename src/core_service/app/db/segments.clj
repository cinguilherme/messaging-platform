(ns core-service.app.db.segments
  (:require [d-core.core.databases.protocols.simple-sql :as sql]
            [next.jdbc.result-set :as rs]))

(defn last-seq-end
  [db conversation-id]
  (let [rows (sql/execute! db
                           ["SELECT max(seq_end) AS max_seq FROM segment_index WHERE conversation_id = ?"
                            conversation-id]
                           {:builder-fn rs/as-unqualified-lower-maps})
        max-seq (:max_seq (first rows))]
    (when max-seq
      (long max-seq))))

(defn insert-segment!
  [db {:keys [conversation-id seq-start seq-end object-key byte-size]}]
  (sql/insert! db {:conversation_id conversation-id
                   :seq_start seq-start
                   :seq_end seq-end
                   :object_key object-key
                   :byte_size byte-size}
               {:table :segment_index}))

(defn list-segments
  [db {:keys [conversation-id before-seq limit]}]
  (let [limit (long (or limit 50))
        [query params] (if before-seq
                         [(str "SELECT conversation_id, seq_start, seq_end, object_key, byte_size "
                               "FROM segment_index "
                               "WHERE conversation_id = ? AND seq_end < ? "
                               "ORDER BY seq_end DESC LIMIT ?")
                          [conversation-id before-seq limit]]
                         [(str "SELECT conversation_id, seq_start, seq_end, object_key, byte_size "
                               "FROM segment_index "
                               "WHERE conversation_id = ? "
                               "ORDER BY seq_end DESC LIMIT ?")
                          [conversation-id limit]])]
    (sql/execute! db (into [query] params)
                  {:builder-fn rs/as-unqualified-lower-maps})))

(defn list-expired-segments
  [db {:keys [cutoff limit]}]
  (let [limit (long (or limit 200))
        cutoff (or cutoff (java.time.Instant/now))
        cutoff-ts (cond
                    (instance? java.sql.Timestamp cutoff) cutoff
                    (instance? java.time.Instant cutoff) (java.sql.Timestamp/from cutoff)
                    (instance? java.util.Date cutoff) (java.sql.Timestamp. (.getTime ^java.util.Date cutoff))
                    (number? cutoff) (java.sql.Timestamp. (long cutoff))
                    :else (java.sql.Timestamp/from (java.time.Instant/now)))]
    (sql/execute! db
                  [(str "SELECT conversation_id, seq_start, object_key, created_at "
                        "FROM segment_index "
                        "WHERE created_at < ? "
                        "ORDER BY created_at ASC LIMIT ?")
                   cutoff-ts limit]
                  {:builder-fn rs/as-unqualified-lower-maps})))

(defn delete-segment!
  [db {:keys [conversation-id seq-start]}]
  (sql/delete! db {:table :segment_index
                   :where {:conversation_id conversation-id
                           :seq_start seq-start}}))
