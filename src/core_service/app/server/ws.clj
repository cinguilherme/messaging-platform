(ns core-service.app.server.ws
  (:require
   [integrant.core :as ig]
   [aleph.http :as http]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [core-service.app.db.conversations :as conversations-db]
   [core-service.app.libs.redis :as redis-lib]
   [core-service.app.server.http :as shttp]
   [duct.logger :as logger]
   [taoensso.carmine :as car]))

;; ---------------------------------------------------------------------------
;; Echo handler (dev / smoke-test)
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :core-service.app.server.ws/handler
  [_ {:keys [logger]}]
  (fn [req]
    (d/let-flow [conn (http/websocket-connection req)]
                (s/consume
                 (fn [msg]
                   (s/put! conn msg))
                 conn)
                conn)))

;; ---------------------------------------------------------------------------
;; WS middleware chain assembler (Ring middleware for the WS Reitit router)
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :core-service.app.server.ws/base-middleware
  [_ {:keys [api-key auth user-context]}]
  (into [] (filter some?) [api-key auth user-context]))

;; ---------------------------------------------------------------------------
;; Conversation stream handler
;; ---------------------------------------------------------------------------

(defn- subscribe-and-stream!
  "Subscribe to the Redis Pub/Sub channel for a conversation and forward
   every published message to the WebSocket connection. Returns the Carmine
   listener (closeable) so the caller can tear it down on disconnect."
  [conn-spec channel ws-conn logger]
  (car/with-new-pubsub-listener conn-spec
    {"message" (fn [[_type _ch payload]]
                 (when-not (s/closed? ws-conn)
                   (s/put! ws-conn payload)))}
    (car/subscribe channel)))

(defmethod ig/init-key :core-service.app.server.ws/conversation-stream
  [_ {:keys [webdeps]}]
  (let [{:keys [db redis naming logger]} webdeps]
    (fn [req]
      (let [conv-id (shttp/parse-uuid (get-in req [:path-params :id]))
            user-id (or (shttp/parse-uuid (get-in req [:auth/principal :subject]))
                        (shttp/parse-uuid (get-in req [:auth/principal :user_id]))
                        (:user-id req))]
        (cond
          (nil? conv-id)
          {:status 400 :body "invalid conversation id"}

          (nil? user-id)
          {:status 401 :body "unauthorized"}

          (not (conversations-db/member? db {:conversation-id conv-id :user-id user-id}))
          {:status 403 :body "forbidden"}

          :else
          (d/let-flow [ws-conn (http/websocket-connection req)]
            (let [channel (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)
                  conn-spec (redis-lib/conn redis)
                  listener (subscribe-and-stream! conn-spec channel ws-conn logger)]
              (when logger
                (logger/log logger :info ::conversation-stream-connected
                            {:conversation-id (str conv-id)
                             :user-id (str user-id)
                             :channel channel}))
              (s/on-closed ws-conn
                (fn []
                  (when logger
                    (logger/log logger :info ::conversation-stream-disconnected
                                {:conversation-id (str conv-id)
                                 :user-id (str user-id)
                                 :channel channel}))
                  (car/close-listener listener)))
              ws-conn)))))))
