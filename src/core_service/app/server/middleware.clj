(ns core-service.app.server.middleware
  (:require [integrant.core :as ig]
            [ring.middleware.multipart-params :as multipart]))

(defmethod ig/init-key :core-service.app.server.middleware/multipart
  [_ _]
  multipart/wrap-multipart-params)
