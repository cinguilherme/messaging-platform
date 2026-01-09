(ns core-service.workers.component
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :core-service.workers.component/worker
  [_ {:keys [name logger]}]
  (logger/log logger :info ::initializing-worker {:name name})
  (println "Initializing worker:" name))

(defmethod ig/halt-key! :core-service.workers.component/worker
  [_ {:keys [name logger]}]
  (logger/log logger :info ::stopping-worker {:name name})
  (println "Stopping worker:" name))