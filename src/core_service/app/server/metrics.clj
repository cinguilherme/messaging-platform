(ns core-service.app.server.metrics
  (:require [clojure.string :as str]
            [d-core.core.metrics.protocol :as metrics]
            [integrant.core :as ig])
  (:import (io.prometheus.client.exporter.common TextFormat)))

(defn- label-value
  [v]
  (cond
    (nil? v) "unknown"
    (keyword? v) (if-let [ns (namespace v)]
                   (str ns "/" (name v))
                   (name v))
    :else (str v)))

(defn- labels->array
  [& values]
  (into-array String (map label-value values)))

(defn- normalize-path
  [uri]
  (-> (or uri "")
      (str/replace #"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}" "/:id")
      (str/replace #"/\\d+" "/:id")))

(defn wrap-http-metrics
  [handler {:keys [metrics http]}]
  (let [{:keys [requests-total request-duration]} http
        metrics-api metrics]
    (fn [req]
      (let [start (System/nanoTime)]
        (try
          (let [resp (handler req)
                duration (/ (double (- (System/nanoTime) start)) 1000000000.0)
                method (some-> (:request-method req) name str/lower-case)
                route (normalize-path (:uri req))
                status (str (or (:status resp) 200))]
            (when requests-total
              (metrics/inc! metrics-api
                            (.labels requests-total (labels->array method route status))))
            (when request-duration
              (metrics/observe! metrics-api
                                (.labels request-duration (labels->array method route))
                                duration))
            resp)
          (catch Throwable t
            (let [duration (/ (double (- (System/nanoTime) start)) 1000000000.0)
                  method (some-> (:request-method req) name str/lower-case)
                  route (normalize-path (:uri req))]
              (when requests-total
                (metrics/inc! metrics-api
                              (.labels requests-total (labels->array method route "500"))))
              (when request-duration
                (metrics/observe! metrics-api
                                  (.labels request-duration (labels->array method route))
                                  duration)))
            (throw t)))))))

(defmethod ig/init-key :core-service.app.server.middleware/metrics
  [_ {:keys [metrics]}]
  (fn [handler]
    (wrap-http-metrics handler metrics)))

(defn metrics-handler
  [{:keys [registry]}]
  (fn [_req]
    (if-not registry
      {:status 500
       :headers {"content-type" "text/plain"}
       :body "metrics registry not configured"}
      (let [writer (java.io.StringWriter.)]
        (TextFormat/write004 writer (.metricFamilySamples registry))
        {:status 200
         :headers {"content-type" TextFormat/CONTENT_TYPE_004}
         :body (str writer)}))))

(defmethod ig/init-key :core-service.app.server.metrics/handler
  [_ {:keys [metrics registry]}]
  (metrics-handler {:registry (or registry (:registry metrics))}))
