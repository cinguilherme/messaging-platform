(ns core-service.handler
  (:require [integrant.core :as ig]
            [core-service.server.routes :as routes]))

(defn health-handler [_req]
  {:status 200
   :headers {"content-type" "application/json"}
   :body "{\"ok\":true}"})