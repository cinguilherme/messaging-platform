(ns core-service.storage.minio
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :core-service.storage.minio/minio
  [_ {:keys [logger]}]
  (logger/log logger :info ::initializing-minio)
  (println "Initializing Minio"))

(defmethod ig/halt-key! :core-service.storage.minio/minio
  [_ {:keys [logger]}]
  (logger/log logger :info ::stopping-minio)
  (println "Stopping Minio"))