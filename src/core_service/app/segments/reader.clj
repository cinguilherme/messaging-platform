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
  "Fetch messages from Minio segments, newest-to-oldest.
  Options: {:limit n :before-seq seq}."
  [{:keys [db minio segments]} conversation-id {:keys [limit before-seq]}]
  (let [limit (long (or limit 50))
        segment-batch (long (or (:segment-batch segments) 10))
        compression (or (:compression segments) :gzip)
        codec (or (:codec segments) :edn)]
    (loop [before before-seq
           remaining limit
           acc []]
      (if (<= remaining 0)
        {:messages acc
         :next-seq (when (seq acc) (:seq (last acc)))
         :has-more? true}
        (let [rows (segments-db/list-segments db {:conversation-id conversation-id
                                                  :before-seq before
                                                  :limit segment-batch})]
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
                                                  filtered (if before
                                                             (filter #(< (:seq %) before) msgs)
                                                             msgs)]
                                              (->> filtered
                                                   (sort-by :seq >)
                                                   vec)))))
                                vec)
                  used (vec (take remaining messages))
                  remaining' (- remaining (count used))
                  acc' (into acc used)
                  next-before (when-let [last-row (last rows)]
                                (:seq_start last-row))
                  has-more? (or (<= remaining' 0)
                                (= (count rows) segment-batch))]
              (if (and next-before (pos? remaining'))
                (recur next-before remaining' acc')
                {:messages acc'
                 :next-seq (when (seq acc') (:seq (last acc')))
                 :has-more? has-more?}))))))))
