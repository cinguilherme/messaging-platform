(ns core-service.unit.attachment-workers-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.workers.attachments]
            [d-core.core.storage.protocol :as p-storage]
            [integrant.core :as ig]))

(defn- mock-storage
  []
  (reify p-storage/StorageProtocol
    (storage-get [_ _ _] {:ok true})
    (storage-put [_ _ _ _] {:ok true})
    (storage-delete [_ _ _] {:ok true})
    (storage-get-bytes [_ _ _] {:ok true :bytes (.getBytes "ok" "UTF-8")})
    (storage-put-bytes [_ _ _ _] {:ok true})
    (storage-list [_ _] {:ok true :items []})))

(deftest attachment-workers-init-validates-storage
  (testing "fails when storage is missing"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"storage not configured"
         (ig/init-key :core-service.app.workers.attachments/system
                      {:webdeps {}
                       :logger nil
                       :metrics nil}))))
  (testing "fails when storage does not implement protocol"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"does not implement StorageProtocol"
         (ig/init-key :core-service.app.workers.attachments/system
                      {:webdeps {:storage {}}
                       :logger nil
                       :metrics nil}))))
  (testing "starts when storage implements protocol"
    (let [system (ig/init-key :core-service.app.workers.attachments/system
                              {:webdeps {:storage (mock-storage)}
                               :logger nil
                               :metrics nil})]
      (is (map? system))
      (is (fn? (:stop! system)))
      (ig/halt-key! :core-service.app.workers.attachments/system system))))
