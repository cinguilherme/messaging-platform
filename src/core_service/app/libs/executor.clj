(ns core-service.app.libs.executor
  (:require [core-service.app.libs.env :as env]
            [integrant.core :as ig]
            [core-service.app.executors.bounded :as bounded])
  (:import [java.util.concurrent Executors]))

;; Managed Bounded Executor Service for core-service
;; Provides a fixed thread pool to avoid thread explosion from Clojure's default 'future' pool.

(defn- parse-positive-int
  [value]
  (let [v (cond
            (number? value) (int value)
            (string? value) (try
                              (Integer/parseInt value)
                              (catch Exception _
                                nil))
            :else nil)]
    (when (and v (pos? v))
      v)))

(defmethod ig/init-key :core-service.app.libs.executor/executor
  [_ {:keys [thread-count] :or {thread-count 20}}]
  (bounded/->BoundedExecutor (Executors/newFixedThreadPool (int thread-count))))

(defmethod ig/halt-key! :core-service.app.libs.executor/executor
  [_ executor]
  (bounded/shutdown! (:executor executor)))

(defmethod ig/init-key :core-service.app.libs.executor/keycloak-profile-fetch-executor
  [_ {:keys [thread-count]}]
  (let [thread-count (or (parse-positive-int thread-count)
                         (parse-positive-int (env/getenv "KEYCLOAK_PROFILE_FETCH_MAX_CONCURRENCY"))
                         8)]
    (bounded/->BoundedExecutor (Executors/newFixedThreadPool thread-count))))

(defmethod ig/halt-key! :core-service.app.libs.executor/keycloak-profile-fetch-executor
  [_ executor]
  (bounded/shutdown! (:executor executor)))
