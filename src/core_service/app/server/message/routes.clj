(ns core-service.app.server.message.routes
  (:require [core-service.app.server.message.authed :as authed]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/messages"
   {:post (authed/messages-create {:webdeps webdeps})
    :get (authed/messages-list {:webdeps webdeps})}])
