(ns core-service.app.server.interceptors.user-context
  (:require [integrant.core :as ig]
            [core-service.app.server.http :as http]))

(defn user-context-interceptor
  []
  {:name ::user-context
   :enter (fn [ctx]
            (let [req (:request ctx)
                  user-id (or (http/parse-uuid (get-in req [:auth/principal :subject]))
                              (http/parse-uuid (get-in req [:auth/principal :user_id])))]
              (update ctx :request assoc :user-id user-id)))})

(defmethod ig/init-key :core-service.app.server.interceptors.user-context/user-context
  [_ _]
  (user-context-interceptor))
