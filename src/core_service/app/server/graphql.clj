(ns core-service.app.server.graphql
  (:require [integrant.core :as ig]))

(def schema
  {:queries
   {:ping
    {:type 'String
     :resolve (fn [_ _ _] "pong")}}})

(defmethod ig/init-key :core-service.app.server.graphql/schema
  [_ _]
  schema)
