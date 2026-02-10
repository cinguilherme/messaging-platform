(ns core-service.app.server.receipt.routes
  (:require [core-service.app.server.receipt.authed :as authed]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/receipts"
   {:post (authed/receipts-create {:webdeps webdeps})}])
