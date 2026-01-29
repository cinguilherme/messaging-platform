(ns core-service.app.config.observability
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [integrant.core :as ig]))

(defn- getenv
  "Returns env var value if present and not blank."
  [k]
  (let [v (some-> (System/getenv k) str/trim)]
    (when (seq v) v)))

(defn- parse-bool
  [value]
  (let [v (some-> value str/lower-case)]
    (cond
      (#{"1" "true" "yes" "y" "on"} v) true
      (#{"0" "false" "no" "n" "off"} v) false
      :else nil)))

(defn- env-bool
  [k]
  (parse-bool (getenv k)))

(defn- parse-long
  [value]
  (when (seq value)
    (try
      (Long/parseLong value)
      (catch Exception _ nil))))

(defn- parse-double
  [value]
  (when (seq value)
    (try
      (Double/parseDouble value)
      (catch Exception _ nil))))

(defn- parse-kw-set
  [value]
  (when (seq value)
    (let [v (str/trim value)
          lower (str/lower-case v)]
      (if (#{"all" "*"} lower)
        #{}
        (->> (str/split v #"[,\s]+")
             (remove str/blank?)
             (map keyword)
             set)))))

(defn- parse-edn
  [value]
  (when (seq value)
    (try
      (edn/read-string value)
      (catch Exception _ nil))))

(defn- deep-merge
  [a b]
  (merge-with (fn [x y]
                (if (and (map? x) (map? y))
                  (deep-merge x y)
                  y))
              a b))

(def default-logging
  {:level :info
   :components #{}
   :workers #{}
   :segment-flush {:enabled true
                   :stage-logs true}
   :segment-retention {:enabled true}
   :ticker {:enabled true
            :tick-log-interval-ms 0}
   :sampling {:enabled false
              :default-rate 1.0
              :per-component {}}
   :redact-fields #{}
   :include-stacktraces false})

(defn- env-overrides
  []
  (let [level (some-> (getenv "OBS_LOG_LEVEL") str/lower-case keyword)
        components (parse-kw-set (getenv "OBS_LOG_COMPONENTS"))
        workers (parse-kw-set (getenv "OBS_LOG_WORKERS"))
        segment-enabled (env-bool "OBS_LOG_SEGMENT_FLUSH_ENABLED")
        segment-stage (env-bool "OBS_LOG_SEGMENT_FLUSH_STAGE_LOGS")
        retention-enabled (env-bool "OBS_LOG_RETENTION_ENABLED")
        ticker-enabled (env-bool "OBS_LOG_TICKER_ENABLED")
        ticker-interval (parse-long (getenv "OBS_LOG_TICKER_INTERVAL_MS"))
        sampling-enabled (env-bool "OBS_LOG_SAMPLING_ENABLED")
        sampling-default (parse-double (getenv "OBS_LOG_SAMPLING_DEFAULT_RATE"))
        sampling-per (parse-edn (getenv "OBS_LOG_SAMPLING_PER_COMPONENT"))
        redact-fields (parse-kw-set (getenv "OBS_LOG_REDACT_FIELDS"))
        stacktraces (env-bool "OBS_LOG_INCLUDE_STACKTRACES")]
    (cond-> {}
      level (assoc :level level)
      components (assoc :components components)
      workers (assoc :workers workers)
      (some? segment-enabled) (assoc-in [:segment-flush :enabled] segment-enabled)
      (some? segment-stage) (assoc-in [:segment-flush :stage-logs] segment-stage)
      (some? retention-enabled) (assoc-in [:segment-retention :enabled] retention-enabled)
      (some? ticker-enabled) (assoc-in [:ticker :enabled] ticker-enabled)
      (some? ticker-interval) (assoc-in [:ticker :tick-log-interval-ms] ticker-interval)
      (some? sampling-enabled) (assoc-in [:sampling :enabled] sampling-enabled)
      (some? sampling-default) (assoc-in [:sampling :default-rate] sampling-default)
      (map? sampling-per) (assoc-in [:sampling :per-component] sampling-per)
      redact-fields (assoc :redact-fields redact-fields)
      (some? stacktraces) (assoc :include-stacktraces stacktraces))))

(defmethod ig/init-key :core-service.app.config.observability/logging
  [_ opts]
  (deep-merge default-logging (deep-merge (env-overrides) opts)))
