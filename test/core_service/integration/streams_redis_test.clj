(ns core-service.integration.streams-redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.streams.redis :as streams]
            [d-core.core.clients.redis]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn- redis-up?
  [redis-client]
  (try
    (let [resp (car/wcar (:conn redis-client) (car/ping))]
      (= "PONG" resp))
    (catch Exception _ false)))

(deftest stream-append-and-read
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [stream (str "test:stream:" (java.util.UUID/randomUUID))
            payload-1 (.getBytes "one" "UTF-8")
            payload-2 (.getBytes "two" "UTF-8")]
        (try
          (streams/append! redis-client stream payload-1)
          (streams/append! redis-client stream payload-2)
          (let [{:keys [entries next-cursor]} (streams/read! redis-client stream {:direction :backward :limit 1})
                payload (-> entries first :payload)
                cursor next-cursor]
            (testing "first page returns latest"
              (is (= "two" (String. ^bytes payload "UTF-8")))
              (is (string? cursor)))
            (let [{:keys [entries]} (streams/read! redis-client stream {:direction :backward
                                                                        :limit 1
                                                                        :cursor cursor})
                payload (-> entries first :payload)]
            (testing "second page returns older"
              (is (= "one" (String. ^bytes payload "UTF-8"))))))
          (finally
            (car/wcar (:conn redis-client)
              (car/del stream))))))))
