(ns core-service.app.observability.logging
  (:require [duct.logger :as logger]))

(defn- debug-enabled?
  [logging]
  (= :debug (or (:level logging) :info)))

(defn- component-enabled?
  [logging component]
  (let [components (:components logging)]
    (or (not (seq components))
        (and component (contains? components component)))))

(defn- worker-enabled?
  [logging worker]
  (let [workers (:workers logging)]
    (or (not (seq workers))
        (and worker (contains? workers worker)))))

(defn- component-config-enabled?
  [logging component]
  (let [config (get logging component)]
    (not (and (map? config) (false? (:enabled config))))))

(defn- sample-rate
  [logging component]
  (let [rate (or (get-in logging [:sampling :per-component component])
                 (get-in logging [:sampling :default-rate])
                 1.0)]
    (double rate)))

(defn- sampled?
  [logging component]
  (if (get-in logging [:sampling :enabled])
    (let [rate (sample-rate logging component)]
      (cond
        (<= rate 0.0) false
        (>= rate 1.0) true
        :else (< (rand) rate)))
    true))

(defn- redact-fields
  [logging fields]
  (let [redact (or (:redact-fields logging) #{})]
    (if (seq redact)
      (apply dissoc fields redact)
      fields)))

(defn log-enabled?
  [logging level component worker]
  (and (or (not= level :debug) (debug-enabled? logging))
       (component-enabled? logging component)
       (worker-enabled? logging worker)
       (component-config-enabled? logging component)
       (sampled? logging component)))

(defn log!
  [logger logging level event fields]
  (let [component (:component fields)
        worker (:worker fields)]
    (when (and logger (log-enabled? logging level component worker))
      (logger/log logger level event (redact-fields logging fields)))))

(defn tick-enabled?
  [logging]
  (get-in logging [:ticker :enabled] true))

(defn tick-interval-ms
  [logging]
  (long (or (get-in logging [:ticker :tick-log-interval-ms]) 0)))

(defn stage-logs-enabled?
  [logging component]
  (true? (get-in logging [component :stage-logs])))
