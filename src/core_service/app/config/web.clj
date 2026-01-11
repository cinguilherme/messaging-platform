(ns core-service.app.config.web
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.app.config.web/module
  [_ cfg]
  cfg)

