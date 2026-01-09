(ns core-service.handlers
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.handlers/log-consumed
  [_ _opts]
  (fn [envelope]
    (println "Handled envelope:" envelope)))

