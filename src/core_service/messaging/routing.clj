(ns core-service.messaging.routing
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.messaging/routing
  [_ routing]
  routing)

(def config-map 
  {:defaults {:source :redis}
   :topics {:default {:source :redis
                     :stream "core:default"
                     :group "core"}
           :test-queue {:source :in-memory}}
   :subscriptions {:default {:source :redis
                            :topic :default
                            :handler :core-service.handlers.handlers/log-consumed
                            :options {:block-ms 5000}}}})

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

