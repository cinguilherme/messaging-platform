(ns core-service.consumers.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [duct.logger :as logger]
            [core-service.messaging.codec :as codec]
            [core-service.messaging.routing :as routing]))

(defn- ensure-consumer-group!
  [conn stream group]
  (try
    (car/wcar conn
      ;; Create group starting at 0, create stream if missing.
      (car/xgroup-create stream group "0" "MKSTREAM"))
    (catch Exception _e
      ;; Ignore BUSYGROUP and similar startup races.
      nil)))

(defn- start-redis-subscription!
  [{:keys [subscription-id conn stream group consumer-name codec handler stop? block-ms logger]}]
  (future
    (logger/log logger :report ::redis-subscription-started
                {:id subscription-id :stream stream :group group :consumer consumer-name})
    (ensure-consumer-group! conn stream group)
    (while (not @stop?)
      (let [resp (car/wcar conn
                   ;; BLOCK for up to block-ms. COUNT 1 for now.
                   (car/xreadgroup "GROUP" group consumer-name
                                   "BLOCK" (str block-ms)
                                   "COUNT" "1"
                                   "STREAMS" stream ">"))]
        ;; Response shape: [[stream [[id [field value ...]] ...]]]
        (doseq [[_stream entries] resp
                [id fields] entries]
          (let [m (apply hash-map fields)
                payload (get m "payload")
                envelope (codec/decode codec payload)]
            (handler envelope)
            (car/wcar conn (car/xack stream group id))))))
    (logger/log logger :report ::redis-subscription-stopped {:id subscription-id})))

(defmethod ig/init-key :core-service.consumers.redis/runtime
  [_ {:keys [redis routing codec logger]
      :or {}}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        redis-subs (into {}
                         (filter (fn [[_id sub]] (= :redis (:source sub))))
                         subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)
                           topic-cfg (routing/topic-config routing topic)
                           stream (or (:stream topic-cfg) (str "core:" (name topic)))
                           group (or (:group topic-cfg) "core")
                           consumer-name (or (:consumer options) (str "core-service-" (java.util.UUID/randomUUID)))
                           block-ms (or (:block-ms options) 5000)]
                       [subscription-id
                        (start-redis-subscription! {:subscription-id subscription-id
                                                    :conn (:conn redis)
                                                    :stream stream
                                                    :group group
                                                    :consumer-name consumer-name
                                                    :codec codec
                                                    :handler handler
                                                    :stop? stop?
                                                    :block-ms block-ms
                                                    :logger logger})])))
              redis-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :core-service.consumers.redis/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-redis-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)

