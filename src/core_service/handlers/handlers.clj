(ns core-service.handlers.handlers
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.handlers.handlers/log-consumed
  [_ _opts]
  (fn [envelope]
    (println "Handled envelope:" envelope)))

