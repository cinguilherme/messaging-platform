(ns core-service.storage.bucket
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :core-service.storage.bucket/bucket
  [_ {:keys [logger]}]
  (logger/log logger :info ::initializing-bucket)
  (println "Initializing Bucket"))

(defmethod ig/halt-key! :core-service.storage.bucket/bucket
  [_ {:keys [logger]}]
  (logger/log logger :info ::stopping-bucket)
  (println "Stopping Bucket"))