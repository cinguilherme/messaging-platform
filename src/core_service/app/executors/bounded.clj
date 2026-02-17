(ns core-service.app.executors.bounded
  (:require [core-service.app.executors.protocol :as p])
  (:import [java.util.concurrent ExecutorService Future TimeUnit]))

;; Implementation of Bounded Executor that hides Java interop

(defrecord BoundedTask [^Future f]
  p/Task
  (wait-for [_ timeout-ms timeout-val]
    (try
      (.get f (long timeout-ms) TimeUnit/MILLISECONDS)
      (catch java.util.concurrent.TimeoutException _
        timeout-val)
      (catch Exception _
        ::error)))
  (cancel [_]
    (.cancel f true)))

(defrecord BoundedExecutor [executor]
  p/Executor
  (execute [_ task-fn]
    (->BoundedTask (.submit ^ExecutorService executor
                            ^java.util.concurrent.Callable task-fn))))

(defn shutdown! [executor]
  (let [^ExecutorService executor executor]
    (.shutdown executor)
    (when-not (.awaitTermination executor 5 TimeUnit/SECONDS)
      (.shutdownNow executor))))
