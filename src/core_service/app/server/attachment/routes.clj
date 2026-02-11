(ns core-service.app.server.attachment.routes
  (:require [core-service.app.server.attachment.authed :as attachment-authed]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/attachments"
   {:post (attachment-authed/attachments-create {:webdeps webdeps})}])
