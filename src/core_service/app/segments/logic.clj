(ns core-service.app.segments.logic
  (:require [core-service.app.db.segments :as segments-db]
            [core-service.app.segments.format :as segment-format]
            [d-core.core.storage.protocol :as p-storage]))

(defn missing-object?
  "Determines if a Minio error result indicates a missing object."
  [result]
  (let [error-type (:error-type result)
        error-code (:error-code result)
        status (:status result)]
    (or (= error-type :not-found)
        (= error-code "NoSuchKey")
        (= error-code "NotFound")
        (= 404 status))))

(defn fetch-conv-rows
  "Fetch segment rows for a conversation in the specified direction and batch size."
  [direction db conversation-id cursor segment-batch]
  (if (= direction :forward)
    (segments-db/list-segments-forward db {:conversation-id conversation-id
                                           :after-seq cursor
                                           :limit segment-batch})
    (segments-db/list-segments db {:conversation-id conversation-id
                                   :before-seq cursor
                                   :limit segment-batch})))

(defn segment-messages
  "Fetch and decode messages from a segment. Returns a sequence of message maps."
  [{:keys [db minio]} {:keys [conversation_id seq_start object_key]}
   {:keys [compression codec cursor direction]}]
  (let [obj (p-storage/storage-get-bytes minio object_key {})]
    (cond
      (:ok obj)
      (let [decoded (segment-format/decode-segment (:bytes obj)
                                                   {:compression compression
                                                    :codec codec})
            msgs (:messages decoded)
            filtered (cond
                       (= direction :forward)
                       (if cursor
                         (filter #(> (:seq %) cursor) msgs)
                         msgs)
                       :else
                       (if cursor
                         (filter #(< (:seq %) cursor) msgs)
                         msgs))]
        (->> filtered
             (sort-by :seq (if (= direction :forward) < >))
             vec))
      (missing-object? obj)
      (do
        (segments-db/delete-segment! db {:conversation-id conversation_id
                                         :seq-start seq_start})
        [])
      :else [])))

(defn rows->messages
  "Convert database rows to messages by fetching and decoding segments. Returns a sequence of message maps."
  [db minio compression codec cursor direction rows]
  (->> rows
       (mapcat (fn [row]
                 (segment-messages {:db db :minio minio}
                                   row
                                   {:compression compression
                                    :codec codec
                                    :cursor cursor
                                    :direction direction})))
       vec))
