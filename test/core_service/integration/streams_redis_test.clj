(ns core-service.integration.streams-redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.stream.protocol :as p-stream]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(deftest stream-append-and-read
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            backend (helpers/init-streams-backend redis-client naming)
            stream (str "test:stream:" (java.util.UUID/randomUUID))
            payload-1 (.getBytes "one" "UTF-8")
            payload-2 (.getBytes "two" "UTF-8")]
        (try
          (p-stream/append-payload! backend stream payload-1)
          (p-stream/append-payload! backend stream payload-2)
          (let [{:keys [entries next-cursor]} (p-stream/read-payloads backend stream {:direction :backward :limit 1})
                payload (-> entries first :payload)
                cursor next-cursor]
            (testing "first page returns latest"
              (is (= "two" (String. ^bytes payload "UTF-8")))
              (is (string? cursor)))
            (let [{:keys [entries]} (p-stream/read-payloads backend stream {:direction :backward
                                                                            :limit 1
                                                                            :cursor cursor})
                  payload (-> entries first :payload)]
              (testing "second page returns older"
                (is (= "one" (String. ^bytes payload "UTF-8"))))))
          (finally
            (car/wcar (:conn redis-client)
                      (car/del stream))))))))
