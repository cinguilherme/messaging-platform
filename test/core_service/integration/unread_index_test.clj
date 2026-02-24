(ns core-service.integration.unread-index-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.redis.unread-index :as unread-index]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn- cleanup-unread-index!
  [redis-client naming conversation-id]
  (car/wcar (:conn redis-client)
            (car/del (unread-index/message-index-key naming conversation-id))
            (car/del (unread-index/message-seq-key naming conversation-id))
            (car/del (unread-index/last-read-key naming conversation-id))))

(defn- make-message-ids
  [n]
  (vec (repeatedly n #(java.util.UUID/randomUUID))))

(deftest unread-index-count-and-monotonic-last-read
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            conversation-id (java.util.UUID/randomUUID)
            user-id (java.util.UUID/randomUUID)
            message-ids (make-message-ids 5)
            deps {:redis redis-client :naming naming}]
        (try
          (cleanup-unread-index! redis-client naming conversation-id)
          (doseq [[seq message-id] (map vector (range 1 6) message-ids)]
            (unread-index/index-message! deps {:conversation-id conversation-id
                                               :message-id message-id
                                               :seq seq}))

          (testing "unread is nil when last-read is absent"
            (is (nil? (unread-index/unread-count deps conversation-id user-id))))

          (testing "count is based on seq greater than last-read"
            (unread-index/update-last-read-seq! deps conversation-id user-id 2)
            (is (= 3 (unread-index/unread-count deps conversation-id user-id))))

          (testing "last-read does not move backward"
            (unread-index/update-last-read-seq! deps conversation-id user-id 1)
            (is (= 3 (unread-index/unread-count deps conversation-id user-id))))

          (testing "updating last-read by message id works"
            (unread-index/update-last-read-by-message! deps conversation-id user-id (nth message-ids 3))
            (is (= 1 (unread-index/unread-count deps conversation-id user-id))))

          (finally
            (cleanup-unread-index! redis-client naming conversation-id)))))))

(deftest unread-index-trim-keep-n-removes-old-members
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            conversation-id (java.util.UUID/randomUUID)
            user-id (java.util.UUID/randomUUID)
            message-ids (make-message-ids 5)
            deps {:redis redis-client :naming naming}]
        (try
          (cleanup-unread-index! redis-client naming conversation-id)
          (doseq [[seq message-id] (map vector (range 1 6) message-ids)]
            (unread-index/index-message! deps {:conversation-id conversation-id
                                               :message-id message-id
                                               :seq seq}))

          (unread-index/trim-index! deps {:conversation-id conversation-id
                                          :trim-min-entries 2})
          (unread-index/update-last-read-seq! deps conversation-id user-id 0)

          (testing "only latest N entries remain after rank trim"
            (is (= 2 (unread-index/unread-count deps conversation-id user-id)))
            (is (nil? (unread-index/message-seq deps conversation-id (first message-ids))))
            (is (= 5 (unread-index/message-seq deps conversation-id (last message-ids)))))

          (finally
            (cleanup-unread-index! redis-client naming conversation-id)))))))

(deftest unread-index-trim-by-seq-end
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            conversation-id (java.util.UUID/randomUUID)
            user-id (java.util.UUID/randomUUID)
            message-ids (make-message-ids 5)
            deps {:redis redis-client :naming naming}]
        (try
          (cleanup-unread-index! redis-client naming conversation-id)
          (doseq [[seq message-id] (map vector (range 1 6) message-ids)]
            (unread-index/index-message! deps {:conversation-id conversation-id
                                               :message-id message-id
                                               :seq seq}))

          (unread-index/trim-index! deps {:conversation-id conversation-id
                                          :seq-end 3
                                          :trim-min-entries 0})
          (unread-index/update-last-read-seq! deps conversation-id user-id 0)

          (testing "score-based trim removes entries up to seq-end"
            (is (= 2 (unread-index/unread-count deps conversation-id user-id)))
            (is (nil? (unread-index/message-seq deps conversation-id (first message-ids))))
            (is (= 4 (unread-index/message-seq deps conversation-id (nth message-ids 3)))))

          (finally
            (cleanup-unread-index! redis-client naming conversation-id)))))))
