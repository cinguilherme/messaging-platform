(ns core-service.messaging.dead-letter
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.storage.protocol :as storage]
            [core-service.producers.protocol :as producer]
            [cheshire.core :as json]))

(defprotocol DeadLetterProtocol
  (send-dead-letter! [this envelope error-info opts]
    "Send a failed message to the dead letter sink.
     - envelope: the original message envelope
     - error-info: a map with :error (exception or msg) and :stacktrace
     - opts: runtime options (e.g., override sink)"))

;; Logger Sink
(defrecord LoggerDeadLetter [logger]
  DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info _opts]
    (logger/log logger :error ::dead-letter-logged
                {:envelope envelope
                 :error-info error-info})
    {:ok true :sink :logger}))

(defmethod ig/init-key :core-service.messaging.dead-letter/logger
  [_ {:keys [logger]}]
  (->LoggerDeadLetter logger))

;; Storage Sink
(defrecord StorageDeadLetter [storage logger]
  DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [timestamp (System/currentTimeMillis)
          filename (str "dead-letters/dlq-" timestamp "-" (java.util.UUID/randomUUID) ".json")
          payload (json/generate-string {:envelope envelope :error-info error-info})]
      (try
        (storage/storage-put storage filename payload opts)
        {:ok true :sink :storage :path filename}
        (catch Exception e
          (logger/log logger :error ::storage-dlq-failed {:error (.getMessage e)})
          {:ok false :error (.getMessage e)})))))

(defmethod ig/init-key :core-service.messaging.dead-letter/storage
  [_ {:keys [storage logger]}]
  (->StorageDeadLetter storage logger))

;; Producer Sink
(defrecord ProducerDeadLetter [producer dlq-topic max-retries delay-ms logger]
  DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [dlq-topic (or (:dlq-topic opts) dlq-topic "dead-letters")
          delay-ms (or (:delay-ms opts) delay-ms 0)
          max-retries (or (:max-retries opts) max-retries 3)
          ;; Extract retry count from envelope if it was already retried
          current-retry (get-in envelope [:msg :retry-count] 0)
          next-retry (inc current-retry)]
      (if (> next-retry max-retries)
        (do
          (logger/log logger :error ::max-retries-exceeded 
                      {:topic dlq-topic :retry-count current-retry})
          ;; Fallback to logging the message so it isn't lost
          {:ok false :error :max-retries-exceeded})
        (let [payload {:original-envelope envelope
                       :error-info error-info
                       :retry-count next-retry
                       :failed-at (System/currentTimeMillis)}]
          (try
            (if (> delay-ms 0)
              (do
                (logger/log logger :info ::delaying-dead-letter {:delay-ms delay-ms :retry-count next-retry})
                (future 
                  (Thread/sleep delay-ms)
                  (producer/produce! producer payload {:topic dlq-topic})))
              (producer/produce! producer payload {:topic dlq-topic}))
            {:ok true :sink :producer :topic dlq-topic :retry-count next-retry}
            (catch Exception e
              (logger/log logger :error ::producer-dlq-failed {:error (.getMessage e)})
              {:ok false :error (.getMessage e)})))))))

(defmethod ig/init-key :core-service.messaging.dead-letter/producer
  [_ {:keys [producer dlq-topic max-retries delay-ms logger] 
      :or {dlq-topic "dead-letters"
           max-retries 3
           delay-ms 0}}]
  (->ProducerDeadLetter producer dlq-topic max-retries delay-ms logger))

;; Common Facade
(defrecord CommonDeadLetter [default-sink-key sinks logger]
  DeadLetterProtocol
  (send-dead-letter! [this envelope error-info opts]
    (let [sink-key (or (:sink opts) default-sink-key)
          delegate (get sinks sink-key)]
      (if delegate
        (do
          (logger/log logger :info ::sending-to-dlq {:sink sink-key})
          (let [res (send-dead-letter! delegate envelope error-info opts)]
            (if (and (not (:ok res)) (= sink-key :producer))
              ;; If producer fails (e.g. max retries), fallback to logger
              (send-dead-letter! this envelope (assoc error-info :dlq-error (:error res)) (assoc opts :sink :logger))
              res)))
        (do
          (logger/log logger :error ::missing-dlq-sink {:sink sink-key})
          {:ok false :error :missing-sink})))))

(defmethod ig/init-key :core-service.messaging.dead-letter/common
  [_ {:keys [default-sink sinks logger] :or {default-sink :logger}}]
  (->CommonDeadLetter default-sink sinks logger))
