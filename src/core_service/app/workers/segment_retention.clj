(ns core-service.app.workers.segment-retention
  (:require [core-service.app.db.segments :as segments-db]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.storage.minio :as minio]
            [d-core.libs.workers :as workers]
            [duct.logger :as logger]
            [integrant.core :as ig]))

(def retention-component :segment-retention)
(def retention-worker :segments-retention)

(defn- now-ms []
  (System/currentTimeMillis))

(defn cleanup!
  [{:keys [db minio retention logger metrics] :as components}]
  (let [{:keys [max-age-ms batch-size]} retention
        max-age-ms (long (or max-age-ms 0))
        batch-size (long (or batch-size 200))]
    (if (<= max-age-ms 0)
      {:status :skipped :reason :disabled}
      (let [cutoff (java.time.Instant/ofEpochMilli (- (System/currentTimeMillis) max-age-ms))
            rows (segments-db/list-expired-segments db {:cutoff cutoff
                                                        :limit batch-size})
            result (reduce
                    (fn [acc {:keys [conversation_id seq_start object_key]}]
                      (let [deleted (minio/delete-object! {:storage minio :metrics metrics} object_key)]
                        (if (:ok deleted)
                          (do
                            (segments-db/delete-segment! db {:conversation-id conversation_id
                                                             :seq-start seq_start})
                            (update acc :deleted inc))
                          (update acc :failed inc))))
                    {:deleted 0 :failed 0}
                    rows)]
        (merge {:status :ok
                :checked (count rows)
                :cutoff cutoff}
               result)))))

(defn segment-retention-worker
  [{:keys [components]} _msg]
  (let [{:keys [logger logging]} components
        tick-id (str (java.util.UUID/randomUUID))
        tick-started-at (now-ms)
        log-ctx {:component retention-component
                 :worker retention-worker
                 :tick-id tick-id
                 :tick-started-at tick-started-at}]
    (when (obs-log/tick-enabled? logging)
      (obs-log/log! logger logging :debug ::retention-tick-start log-ctx))
    (try
      (let [result (cleanup! components)
            tick-ended-at (now-ms)
            duration-ms (- tick-ended-at tick-started-at)]
        (obs-log/log! logger logging :info ::retention-tick-complete
                      (merge log-ctx result
                             {:tick-ended-at tick-ended-at
                              :duration-ms duration-ms
                              :success (not= :error (:status result))}))
        result)
      (catch Exception e
        (obs-log/log! logger logging :error ::retention-tick-failed
                      (merge log-ctx
                             {:duration-ms (- (now-ms) tick-started-at)
                              :error (.getMessage e)}))
        (throw e)))))

(def default-definition
  {:channels {:segments/retention-ticks {:buffer 1}
              :workers/errors {:buffer 4}}
   :workers {:segments-retention-ticker {:kind :ticker
                                         :interval-ms 3600000
                                         :out :segments/retention-ticks
                                         :dispatch :go}
             :segments-retention {:kind :command
                                  :in :segments/retention-ticks
                                  :worker-fn segment-retention-worker
                                  :dispatch :thread
                                  :fail-chan :workers/errors}}})

(defn- apply-retention-interval
  [definition interval-ms]
  (if (some? interval-ms)
    (assoc-in definition [:workers :segments-retention-ticker :interval-ms] interval-ms)
    definition))

(defmethod ig/init-key :core-service.app.workers.segment-retention/system
  [_ {:keys [logger db minio retention definition metrics logging]}]
  (let [retention (merge {:max-age-ms 2592000000
                          :batch-size 200
                          :interval-ms 3600000}
                         retention)
        definition (-> (or definition default-definition)
                       (apply-retention-interval (:interval-ms retention)))]
    (logger/log logger :info ::initializing-retention-workers
                {:channels (keys (:channels definition))
                 :workers (keys (:workers definition))})
    (workers/start-workers definition {:logger logger
                                       :metrics metrics
                                       :observability metrics
                                       :logging logging
                                       :db db
                                       :minio minio
                                       :retention retention})))

(defmethod ig/halt-key! :core-service.app.workers.segment-retention/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
