(ns core-service.handler
  (:require [integrant.core :as ig]
            [core-service.routes :as routes]))

(defn health-handler [_req]
  {:status 200
   :headers {"content-type" "application/json"}
   :body "{\"ok\":true}"})

#_(defmethod ig/init-key :core-service.handler/index
  [_ _opts]
  (routes/index {}))

#_(defmethod ig/init-key :core-service.handler/health
  [_ _opts]
  health-handler)