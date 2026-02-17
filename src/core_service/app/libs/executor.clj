(ns core-service.app.libs.executor
  (:require [integrant.core :as ig]
            [core-service.app.executors.bounded :as bounded])
  (:import [java.util.concurrent Executors]))

;; Managed Bounded Executor Service for core-service
;; Provides a fixed thread pool to avoid thread explosion from Clojure's default 'future' pool.

(defmethod ig/init-key :core-service.app.libs.executor/executor
  [_ {:keys [thread-count] :or {thread-count 20}}]
  (bounded/->BoundedExecutor (Executors/newFixedThreadPool (int thread-count))))

(defmethod ig/halt-key! :core-service.app.libs.executor/executor
  [_ executor]
  (bounded/shutdown! (:executor executor)))
