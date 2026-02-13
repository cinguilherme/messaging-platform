(ns core-service.unit.attachment-workers-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.db.attachments :as attachments-db]
            [core-service.app.workers.attachments :as attachment-workers]
            [d-core.core.storage.protocol :as p-storage]
            [integrant.core :as ig])
  (:import (java.awt.image BufferedImage)
           (java.io ByteArrayOutputStream)
           (javax.imageio ImageIO)))

(defn- mock-storage
  []
  (reify p-storage/StorageProtocol
    (storage-get [_ _ _] {:ok true})
    (storage-put [_ _ _ _] {:ok true})
    (storage-delete [_ _ _] {:ok true})
    (storage-get-bytes [_ _ _] {:ok true :bytes (.getBytes "ok" "UTF-8")})
    (storage-put-bytes [_ _ _ _] {:ok true})
    (storage-list [_ _] {:ok true :items []})))

(defn- tiny-png-bytes
  []
  (let [image (BufferedImage. 1 1 BufferedImage/TYPE_INT_RGB)
        out (ByteArrayOutputStream.)]
    (ImageIO/write image "png" out)
    (.toByteArray out)))

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

(deftest image-storer-updates-db-metadata-only-for-standard-variant
  (let [calls (atom [])
        bytes (.getBytes "optimized-image-bytes" "UTF-8")
        ctx {:components {:storage (mock-storage)
                          :db :db
                          :logger nil}}]
    (with-redefs [attachments-db/update-attachment-storage-metadata!
                  (fn [_db args]
                    (swap! calls conj args)
                    {:updated 1})]
      (attachment-workers/image-storer ctx {:attachment-id #uuid "00000000-0000-0000-0000-000000000001"
                                            :object-key "attachments/image/a.jpg"
                                            :bytes bytes
                                            :content-type "image/jpeg"
                                            :variant :standard})
      (attachment-workers/image-storer ctx {:attachment-id #uuid "00000000-0000-0000-0000-000000000001"
                                            :object-key "attachments/image/a.jpg"
                                            :bytes bytes
                                            :content-type "image/jpeg"
                                            :variant :original}))
    (testing "db update happens once, only for :standard overwrite"
      (is (= 1 (count @calls)))
      (is (= #uuid "00000000-0000-0000-0000-000000000001"
             (:attachment-id (first @calls))))
      (is (= (alength ^bytes bytes)
             (:size-bytes (first @calls))))
      (is (string? (:checksum (first @calls))))
      (is (= 64 (count (:checksum (first @calls))))))))

(deftest attachment-orchestrator-reports-queue-failures
  (let [bytes (tiny-png-bytes)
        closed-store (doto (clojure.core.async/chan 1) clojure.core.async/close!)
        open-resize (clojure.core.async/chan 1)]
    (testing "returns error when original store enqueue fails immediately"
      (let [result (attachment-workers/attachment-orchestrator
                    {:channels {:attachments/store closed-store
                                :attachments/resize open-resize}
                     :components {:processing attachment-workers/default-processing
                                  :logger nil
                                  :metrics nil}}
                    {:attachment-id #uuid "00000000-0000-0000-0000-000000000100"
                     :conversation-id #uuid "00000000-0000-0000-0000-000000000101"
                     :object-key "attachments/image/a.png"
                     :bytes bytes
                     :mime-type "image/png"
                     :kind :image})]
        (is (= :error (:status result)))
        (is (false? (get-in result [:enqueue :original])))))
    (clojure.core.async/close! open-resize))
  (let [bytes (tiny-png-bytes)
        open-store (clojure.core.async/chan 1)
        closed-resize (doto (clojure.core.async/chan 1) clojure.core.async/close!)]
    (testing "continues with queued original and exposes failed alt enqueue"
      (let [result (attachment-workers/attachment-orchestrator
                    {:channels {:attachments/store open-store
                                :attachments/resize closed-resize}
                     :components {:processing attachment-workers/default-processing
                                  :logger nil
                                  :metrics nil}}
                    {:attachment-id #uuid "00000000-0000-0000-0000-000000000110"
                     :conversation-id #uuid "00000000-0000-0000-0000-000000000111"
                     :object-key "attachments/image/b.png"
                     :bytes bytes
                     :mime-type "image/png"
                     :kind :image})]
        (is (= :queued (:status result)))
        (is (true? (get-in result [:enqueue :original])))
        (is (false? (get-in result [:enqueue :alt])))))
    (clojure.core.async/close! open-store)))
