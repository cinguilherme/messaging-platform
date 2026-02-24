(ns core-service.app.server.interceptors.metrics
  (:require [clojure.string :as str]
            [core-service.app.libs.time :as time]
            [integrant.core :as ig]
            [d-core.core.metrics.protocol :as d-metrics]))

(defn metrics-interceptor
  [{:keys [metrics]}]
  (let [{:keys [requests-total request-duration]} (:http metrics)
        metrics-api (:metrics metrics)]
    {:name ::metrics
     :enter (fn [ctx]
              (assoc ctx ::start-time (time/now-nanos)))
     :leave (fn [ctx]
              (let [start (::start-time ctx)
                    duration (time/nano-span->seconds start)
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
