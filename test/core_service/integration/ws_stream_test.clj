(ns core-service.integration.ws-stream-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.server.ws :as ws]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [integrant.core :as ig]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [taoensso.carmine :as car]))

(deftest ws-stream-pubsub-delivers-text
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            conv-id (java.util.UUID/randomUUID)
            channel (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)
            ws-conn (s/stream 10)
            payload "{:type :text :body {:text \"hello\"}}"
            payload-bytes (.getBytes payload "UTF-8")
            listener (#'ws/subscribe-and-stream!
                      (:conn redis-client)
                      channel
                      ws-conn
                      nil)]
        (try
          (let [result (loop [attempt 0]
                         (car/wcar (:conn redis-client)
                                   (car/publish channel payload-bytes))
                         (let [message @(d/timeout! (s/take! ws-conn) 200 ::timeout)]
                           (cond
                             (= message payload) message
                             (= message ::timeout)
                             (if (< attempt 10)
                               (do (Thread/sleep 50)
                                   (recur (inc attempt)))
                               message)
                             (< attempt 10) (recur (inc attempt))
                             :else message)))]
            (testing "message arrives on ws stream"
              (is (not= ::timeout result) "timed out waiting for ws frame")
              (is (= payload result))))
          (finally
            (car/close-listener listener)
            (s/close! ws-conn)))))))

(deftest ws-stream-pubsub-delivers-json-when-requested
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)]
    (if-not (helpers/redis-up? redis-client)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            conv-id (java.util.UUID/randomUUID)
            channel (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)
            ws-conn (s/stream 10)
            payload "{:type :text :body {:text \"hello\"}}"
            payload-bytes (.getBytes payload "UTF-8")
            listener (#'ws/subscribe-and-stream!
                      (:conn redis-client)
                      channel
                      ws-conn
                      nil
                      :json)]
        (try
          (let [result (loop [attempt 0]
                         (car/wcar (:conn redis-client)
                                   (car/publish channel payload-bytes))
                         (let [message @(d/timeout! (s/take! ws-conn) 200 ::timeout)]
                           (cond
                             (= message ::timeout)
                             (if (< attempt 10)
                               (do (Thread/sleep 50)
                                   (recur (inc attempt)))
                               message)
                             (< attempt 10) (recur (inc attempt))
                             :else message)))]
            (testing "message arrives as JSON frame"
              (is (not= ::timeout result) "timed out waiting for ws frame")
              (is (= {:type "text" :body {:text "hello"}}
                     (json/parse-string result true)))))
          (finally
            (car/close-listener listener)
            (s/close! ws-conn)))))))
