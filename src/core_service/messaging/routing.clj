(ns core-service.messaging.routing
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.messaging/routing
  [_ routing]
  routing)

(defn topic-config
  [routing topic]
  (get-in routing [:topics topic]))

(defn default-source
  [routing]
  (get-in routing [:defaults :source] :in-memory))

(defn source-for-topic
  [routing topic]
  (or (get-in routing [:topics topic :source])
      (default-source routing)))

