(ns core-service.app.segments.reader
  (:require [core-service.app.segments.logic :as s-logic]))

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
        (let [rows (s-logic/fetch-conv-rows direction db conversation-id cursor segment-batch)]
          (if (empty? rows)
            {:messages acc
             :next-seq (when (seq acc) (:seq (last acc)))
             :has-more? false}
            (let [messages (s-logic/rows->messages db minio compression codec cursor direction rows)
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
