(ns core-service.handlers.handlers
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :core-service.handlers.handlers/log-consumed
  [_ {:keys [logger]}]
  (fn [envelope]
    (logger/log logger :info ::envelope-received {:envelope envelope})))

