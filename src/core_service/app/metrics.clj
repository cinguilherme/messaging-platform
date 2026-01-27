(ns core-service.app.metrics
  (:require [d-core.core.metrics.protocol :as metrics]
            [integrant.core :as ig]))

(defn label-value
  [v]
  (cond
    (nil? v) "unknown"
    (keyword? v) (if-let [ns (namespace v)]
                   (str ns "/" (name v))
                   (name v))
    :else (str v)))

(defn labels->array
  [& values]
  (into-array String (map label-value values)))

(defn- http-metrics
  [metrics]
  {:requests-total (metrics/counter metrics {:name :http_requests_total
                                             :help "HTTP requests completed"
                                             :labels [:method :route :status]})
   :request-duration (metrics/histogram metrics {:name :http_request_duration_seconds
                                                 :help "HTTP request duration in seconds"
                                                 :labels [:method :route]
                                                 :buckets [0.005 0.01 0.025 0.05 0.1
                                                           0.25 0.5 1 2 5]})})

(defn- worker-metrics
  [metrics]
  {:drops-total (metrics/counter metrics {:name :worker_drops_total
                                          :help "Worker message drops"
                                          :labels [:worker :channel :reason]})
   :errors-total (metrics/counter metrics {:name :worker_errors_total
                                           :help "Worker errors"
                                           :labels [:worker :kind]})})

(defn- segment-metrics
  [metrics]
  {:flush-total (metrics/counter metrics {:name :segment_flush_total
                                          :help "Segment flush results"
                                          :labels [:status]})
   :flush-duration (metrics/histogram metrics {:name :segment_flush_duration_seconds
                                               :help "Segment flush duration in seconds"
                                               :buckets [0.01 0.025 0.05 0.1 0.25 0.5 1 2 5 10]})
   :segment-bytes (metrics/histogram metrics {:name :segment_size_bytes
                                              :help "Segment size in bytes"
                                              :buckets [16384 32768 65536 131072 262144 524288 1048576]})
   :segment-messages (metrics/histogram metrics {:name :segment_message_count
                                                 :help "Messages per flushed segment"
                                                 :buckets [1 2 5 10 25 50 100 200 500]})})

(defn- emit-worker-event!
  [metrics workers event]
  (let [{:keys [drops-total errors-total]} workers
        {:keys [worker-id channel reason worker-kind] :as event} event
        event-type (:event/type event)]
    (case event-type
      :workers.drop
      (when drops-total
        (metrics/inc! metrics
                      (.labels drops-total (labels->array worker-id channel reason))))
      :workers.error
      (when errors-total
        (metrics/inc! metrics
                      (.labels errors-total (labels->array worker-id worker-kind))))
      nil)))

(defmethod ig/init-key :core-service.app.metrics/component
  [_ {:keys [metrics]}]
  (let [http (http-metrics metrics)
        workers (worker-metrics metrics)
        segments (segment-metrics metrics)]
    {:metrics metrics
     :registry (metrics/registry metrics)
     :http http
     :workers workers
     :segments segments
     :emit (fn [event] (emit-worker-event! metrics workers event))}))
