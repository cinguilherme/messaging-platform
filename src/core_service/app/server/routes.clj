(ns core-service.app.server.routes
  (:refer-clojure :exclude [test])
  (:require [cheshire.core :as json]
            [core-service.core.producers.protocol :as producer]
            [core-service.core.cache.protocol :as cache]
            [core-service.core.storage.protocol :as storage]))

(defn- get-accept-format
  "Determines response format from Accept header or defaults to JSON"
  [req]
  (let [accept (get-in req [:headers "accept"] "application/json")]
    (cond
      (re-find #"application/edn" accept) :edn
      (re-find #"application/json" accept) :json
      :else :json)))

(defn- format-response
  "Formats response data based on requested format"
  [data format]
  (case format
    :edn {:status 200
          :headers {"content-type" "application/edn"}
          :body (pr-str data)}
    :json {:status 200
           :headers {"content-type" "application/json"}
           :body (json/generate-string data)}))

(defn index [_options]
  (fn [_req]
    (format-response {:ok true} (get-accept-format _req))))

(defn test [_options]
  (fn [_req]
    (let [p (:producer _options)
          c (:cache _options)
          s (:storage _options)
          msg {:message "Hello, World!" :timestamp (System/currentTimeMillis)}
          cache-key "last-test-msg"
          storage-key (str "msg-" (:timestamp msg) ".json")

          ;; Cache operations
          cached-msg (cache/cache-lookup c cache-key {})
          newly-cached? (not cached-msg)
          _ (when newly-cached?
              (cache/cache-put c cache-key msg {}))

          ;; Storage operations
          storage-result (storage/storage-put s storage-key (json/generate-string msg) {})

          ack (producer/produce! p msg {:topic :default})
          ;; Produce to a topic that fails once

          ;; sample kafka and jetstream messages
          kafka-msg-ack (producer/produce! p {:type :kafka-test :msg "this is a kafka message"} {:topic :kafka-test})
          jetstream-msg-ack (producer/produce! p {:type :jetstream-test :msg "this is a jetstream message"} {:topic :jetstream-test})
          fail-ack (producer/produce! p {:type :fail-test :msg "this should fail once"} {:topic :to-fail})]
      (format-response {:ok true
                        :msg msg
                        :cached-msg cached-msg
                        :ack ack
                        :kafka-msg-ack kafka-msg-ack
                        :jetstream-msg-ack jetstream-msg-ack
                        :fail-ack fail-ack
                        :cached? (boolean cached-msg)
                        :newly-cached? newly-cached?
                        :storage-result storage-result}
                       (get-accept-format _req)))))

