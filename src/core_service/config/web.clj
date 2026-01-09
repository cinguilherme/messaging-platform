(ns core-service.config.web
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.config.web/module
  [_ cfg]
  cfg)

