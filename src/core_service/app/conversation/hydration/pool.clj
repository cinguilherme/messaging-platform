(ns core-service.app.conversation.hydration.pool
  (:require [core-service.app.executors.bounded :as bounded]
            [integrant.core :as ig])
  (:import [java.util.concurrent Executors]))

(defmethod ig/init-key :core-service.app.conversation.hydration/pool
  [_ {:keys [thread-count] :or {thread-count 20}}]
  (bounded/->BoundedExecutor (Executors/newFixedThreadPool (int thread-count))))

(defmethod ig/halt-key! :core-service.app.conversation.hydration/pool
  [_ pool]
  (bounded/shutdown! (:executor pool)))