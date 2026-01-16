(ns core-service.app.server.ws
  (:require
   [integrant.core :as ig]
   [aleph.http :as http]
   [manifold.deferred :as d]
   [manifold.stream :as s]))

(defmethod ig/init-key :core-service.app.server.ws/handler
  [_ {:keys [logger]}]
  (fn [req]
    (d/let-flow [conn (http/websocket-connection req)]
                ;; basic echo (replace with your protocol: json/edn, etc)
                (s/consume
                 (fn [msg]
                   ;; msg is usually a string
                 (s/put! conn msg))
                 conn)
                conn)))
