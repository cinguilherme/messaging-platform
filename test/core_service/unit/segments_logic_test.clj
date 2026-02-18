(ns core-service.unit.segments-logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.segments.logic :as segments-logic]))

(deftest rows->messages-preserves-row-order-while-flattening
  (testing "rows->messages flattens all segment message vectors while preserving row order"
    (let [rows [{:row-id 1}
                {:row-id 2}
                {:row-id 3}]
          sleep-ms {1 40
                    2 5
                    3 20}]
      (with-redefs [segments-logic/segment-messages (fn [_ {:keys [row-id]} _]
                                                      (Thread/sleep (get sleep-ms row-id 0))
                                                      [{:seq (* row-id 10)}
                                                       {:seq (+ (* row-id 10) 1)}])]
        (is (= [10 11 20 21 30 31]
               (mapv :seq (segments-logic/rows->messages :db :minio :gzip :edn nil :backward rows))))))))
