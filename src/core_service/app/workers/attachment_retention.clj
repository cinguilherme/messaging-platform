(ns core-service.app.workers.attachment-retention
  (:require [clojure.string :as str]
            [core-service.app.db.attachments :as attachments-db]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.server.attachment.logic :as attachment-logic]
            [d-core.core.storage.protocol :as p-storage]
            [d-core.libs.workers :as workers]
            [duct.logger :as logger]
            [integrant.core :as ig]))

(def retention-component :attachment-retention)
(def retention-worker :attachments-retention)

(defn- now-ms []
  (System/currentTimeMillis))

(defn- missing-object?
  [result]
  (let [error-type (:error-type result)
        error-code (:error-code result)
        status (:status result)
        error (some-> (:error result) str)]
    (or (= error-type :not-found)
        (= error-code "NoSuchKey")
        (= error-code "NotFound")
        (= 404 status)
        (= "NoSuchKey" error)
        (= "NotFound" error))))

(defn cleanup!
  [{:keys [db minio retention]}]
  (let [{:keys [max-age-ms batch-size]} retention
        max-age-ms (long (or max-age-ms 0))
        batch-size (long (or batch-size 200))]
    (if (<= max-age-ms 0)
      {:status :skipped :reason :disabled}
      (let [cutoff (java.time.Instant/now)
            rows (attachments-db/list-expired-attachments db {:cutoff cutoff
                                                              :limit batch-size})
            result (reduce (fn [acc {:keys [attachment_id object_key mime_type]}]
                             (let [deleted (p-storage/storage-delete minio object_key {})
                                   original-deleted? (or (:ok deleted) (missing-object? deleted))
                                   image? (str/starts-with? (or mime_type "") "image/")
                                   alt-key (when image?
                                             (attachment-logic/derive-alt-key object_key))]
                               (if original-deleted?
                                 (do
                                   (when alt-key
                                     ;; Low-res variant cleanup is best-effort and must not block row deletion.
                                     (p-storage/storage-delete minio alt-key {}))
                                   (attachments-db/delete-attachment! db {:attachment-id attachment_id})
                                   (update acc :deleted inc))
                                 (update acc :failed inc))))
                           {:deleted 0 :failed 0}
                           rows)]
        (merge {:status :ok
                :checked (count rows)
                :cutoff cutoff}
               result)))))

(defn attachment-retention-worker
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
  {:channels {:attachments/retention-ticks {:buffer 1}
              :workers/errors {:buffer 4}}
   :workers {:attachments-retention-ticker {:kind :ticker
                                            :interval-ms 3600000
                                            :out :attachments/retention-ticks
                                            :dispatch :go}
             :attachments-retention {:kind :command
                                     :in :attachments/retention-ticks
                                     :worker-fn attachment-retention-worker
                                     :dispatch :thread
                                     :fail-chan :workers/errors}}})

(defn- apply-retention-interval
  [definition interval-ms]
  (if (some? interval-ms)
    (assoc-in definition [:workers :attachments-retention-ticker :interval-ms] interval-ms)
    definition))

(defmethod ig/init-key :core-service.app.workers.attachment-retention/system
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

(defmethod ig/halt-key! :core-service.app.workers.attachment-retention/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
