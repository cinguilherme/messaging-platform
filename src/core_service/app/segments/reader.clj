(ns core-service.app.segments.reader
  (:require [core-service.app.db.segments :as segments-db]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.storage.minio :as minio]))

(defn- missing-object?
  [result]
  (let [error-type (:error-type result)
        error-code (:error-code result)
        status (:status result)]
    (or (= error-type :not-found)
        (= error-code "NoSuchKey")
        (= error-code "NotFound")
        (= 404 status))))

(defn- segment-messages
  [{:keys [db minio metrics]} {:keys [conversation_id seq_start object_key]}
   {:keys [compression codec cursor direction]}]
  (let [obj (minio/get-bytes! {:storage minio :metrics metrics} object_key)]
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

(defn fetch-messages
  "Fetch messages from Minio segments.
  Options: {:limit n :cursor seq :direction :backward|:forward}."
  [{:keys [db minio segments metrics] :as components} conversation-id {:keys [limit cursor direction]}]
  (let [limit (long (or limit 50))
        segment-batch (long (or (:segment-batch segments) 10))
        compression (or (:compression segments) :gzip)
        codec (or (:codec segments) :edn)
        direction (or direction :backward)]
    (loop [cursor cursor
           remaining limit
           acc []]
      (if (<= remaining 0)
        {:messages acc
         :next-seq (when (seq acc) (:seq (last acc)))
         :has-more? true}
        (let [rows (if (= direction :forward)
                     (segments-db/list-segments-forward db {:conversation-id conversation-id
                                                            :after-seq cursor
                                                            :limit segment-batch})
                     (segments-db/list-segments db {:conversation-id conversation-id
                                                    :before-seq cursor
                                                    :limit segment-batch}))]
          (if (empty? rows)
            {:messages acc
             :next-seq (when (seq acc) (:seq (last acc)))
             :has-more? false}
            (let [messages (->> rows
                                (mapcat (fn [row]
                                          (segment-messages {:db db :minio minio :metrics metrics}
                                                            row
                                                            {:compression compression
                                                             :codec codec
                                                             :cursor cursor
                                                             :direction direction})))
                                vec)
                  used (vec (take remaining messages))
                  remaining' (- remaining (count used))
                  acc' (into acc used)
                  next-cursor (when-let [last-row (last rows)]
                                (if (= direction :forward)
                                  (:seq_end last-row)
                                  (:seq_start last-row)))
                  has-more? (or (<= remaining' 0)
                                (= (count rows) segment-batch))]
              (if (and next-cursor (pos? remaining'))
                (recur next-cursor remaining' acc')
                {:messages acc'
                 :next-seq (when (seq acc') (:seq (last acc')))
                 :has-more? has-more?}))))))))
