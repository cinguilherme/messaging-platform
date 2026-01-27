(ns core-service.app.segments.reader
  (:require [core-service.app.db.segments :as segments-db]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.storage.minio :as minio]))

(defn- decode-segment
  [minio-client object-key {:keys [compression codec]}]
  (let [obj (minio/get-bytes! minio-client object-key)]
    (when (:ok obj)
      (segment-format/decode-segment (:bytes obj)
                                     {:compression compression
                                      :codec codec}))))

(defn fetch-messages
  "Fetch messages from Minio segments.
  Options: {:limit n :cursor seq :direction :backward|:forward}."
  [{:keys [db minio segments]} conversation-id {:keys [limit cursor direction]}]
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
                                (mapcat (fn [{:keys [object_key]}]
                                          (when-let [decoded (decode-segment minio object_key
                                                                             {:compression compression
                                                                              :codec codec})]
                                            (let [msgs (:messages decoded)
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
                                                   vec)))))
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
