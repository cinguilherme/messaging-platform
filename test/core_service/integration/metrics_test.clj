(ns core-service.integration.metrics-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.server.metrics :as server-metrics]
            [d-core.core.metrics.prometheus]
            [integrant.core :as ig]))

(defn- metrics-component
  []
  (let [registry (ig/init-key :d-core.core.metrics.prometheus/registry {:jvm-metrics? false})
        metrics (ig/init-key :d-core.core.metrics.prometheus/metrics {:registry registry})
        component (ig/init-key :core-service.app.metrics/component {:metrics metrics})]
    (assoc component :registry registry)))

(defn- sample-value
  [registry metric label-names label-values]
  (.getSampleValue registry
                   metric
                   (into-array String label-names)
                   (into-array String label-values)))

(deftest metrics-endpoint-exposes-prometheus
  (let [{:keys [registry]} (metrics-component)
        handler (server-metrics/metrics-handler {:registry registry})
        resp (handler {:request-method :get :uri "/metrics"})]
    (is (= 200 (:status resp)))
    (is (string? (:body resp)))
    (is (str/includes? (:body resp) "http_requests_total"))))

(deftest http-metrics-are-recorded
  (let [{:keys [registry] :as component} (metrics-component)
        handler (server-metrics/wrap-http-metrics (fn [_] {:status 200 :body "ok"})
                                                  {:metrics (:metrics component)
                                                   :http (:http component)})
        conv-id "7c2bcfd2-7d6b-4fd6-9e0a-71a7a2f3d30b"]
    (handler {:request-method :get
              :uri (str "/v1/conversations/" conv-id)})
    (testing "counter incremented"
      (is (= 1.0
             (double (or (sample-value registry
                                       "http_requests_total"
                                       ["method" "route" "status"]
                                       ["get" "/v1/conversations/:id" "200"])
                         0.0)))))
    (testing "histogram recorded"
      (is (= 1.0
             (double (or (sample-value registry
                                       "http_request_duration_seconds_count"
                                       ["method" "route"]
                                       ["get" "/v1/conversations/:id"])
                         0.0)))))))

(deftest worker-metrics-emitted
  (let [{:keys [registry emit]} (metrics-component)]
    (emit {:event/type :workers.drop
           :worker-id :segments-flush
           :channel :segments/flush-ticks
           :reason :input-drop})
    (is (= 1.0
           (double (or (sample-value registry
                                     "worker_drops_total"
                                     ["worker" "channel" "reason"]
                                     ["segments-flush" "segments/flush-ticks" "input-drop"])
                       0.0))))))
