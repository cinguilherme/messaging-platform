(ns core-service.app.server.interceptors.metrics
  (:require [integrant.core :as ig]
            [clojure.string :as str]
            [d-core.core.metrics.protocol :as d-metrics]))

(defn metrics-interceptor
  [{:keys [metrics]}]
  (let [{:keys [requests-total request-duration]} (:http metrics)
        metrics-api (:metrics metrics)]
    {:name ::metrics
     :enter (fn [ctx]
              (assoc ctx ::start-time (System/nanoTime)))
     :leave (fn [ctx]
              (let [start (::start-time ctx)
                    duration (/ (double (- (System/nanoTime) start)) 1000000000.0)
                    req (:request ctx)
                    resp (:response ctx)
                    method (some-> (:request-method req) name str/lower-case)
                    route (or (get-in ctx [:reitit.core/match :template]) (:uri req))
                    status (str (or (:status resp) 200))]
                (try
                  (when (and metrics-api requests-total)
                    (d-metrics/inc! metrics-api
                                    (.labels requests-total (into-array String [(str method) (str route) (str status)]))))
                  (when (and metrics-api request-duration)
                    (d-metrics/observe! metrics-api
                                        (.labels request-duration (into-array String [(str method) (str route)]))
                                        duration))
                  (catch Exception _))
                ctx))}))

(defmethod ig/init-key :core-service.app.server.interceptors.metrics/metrics
  [_ opts]
  (metrics-interceptor opts))
