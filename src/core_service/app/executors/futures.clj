(ns core-service.app.executors.futures
  (:require [core-service.app.executors.protocol :as p]))

(defrecord FutureTask [f]
  p/Task
  (wait-for [_ timeout-ms timeout-val]
    (deref f timeout-ms timeout-val))
  (cancel [_]
    (future-cancel f)))

(defrecord Futures []
  p/Executor
  (execute [this task-fn]
    (->FutureTask (future (task-fn)))))