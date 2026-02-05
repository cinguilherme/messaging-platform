(ns core-service.app.server.interceptors.cid
  (:require [integrant.core :as ig]
            [clojure.string :as str]))

(defn- random-string [len]
  (let [chars "abcdefghijklmnopqrstuvwxyz0123456789"]
    (apply str (repeatedly len #(rand-nth chars)))))

(defn cid-interceptor
  []
  {:name ::cid
   :enter (fn [ctx]
            (let [req (:request ctx)
                  existing-cid (or (get-in req [:headers "x-correlation-id"])
                                   (get-in req [:headers "x-cid"]))
                  new-cid (if (seq existing-cid)
                            (str existing-cid "." (random-string 4))
                            (random-string 4))]
              (-> ctx
                  (assoc :cid new-cid)
                  (update :request assoc :cid new-cid)
                  (update-in [:request :headers] assoc "x-correlation-id" new-cid))))
   :leave (fn [ctx]
            (let [cid (:cid ctx)]
              (update-in ctx [:response :headers] assoc "x-correlation-id" cid)))})

(defmethod ig/init-key :core-service.app.server.interceptors.cid/cid
  [_ _]
  (cid-interceptor))
