(ns core-service.text-search.common
  (:require [duct.logger :as logger]
            [core-service.text-search.protocol :as p]))

(defn- log*
  [maybe-logger level event data]
  (when maybe-logger
    (logger/log maybe-logger level event data)))

(defn- delegate
  [engines default-engine opts]
  (let [engine (or (:engine opts) default-engine)
        backend (get engines engine)]
    (when-not backend
      (throw (ex-info "Unknown text-search engine" {:engine engine :known (keys engines)})))
    [engine backend]))

(defrecord CommonTextSearch [default-engine engines logger]
  p/TextSearchAdminProtocol
  (create-collection! [_ collection-name schema opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :info ::create-collection! {:engine engine :collection collection-name})
      (p/create-collection! backend collection-name schema opts)))
  (drop-collection! [_ collection-name opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :info ::drop-collection! {:engine engine :collection collection-name})
      (p/drop-collection! backend collection-name opts)))
  (upsert-document! [_ collection-name document-id data opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :debug ::upsert-document! {:engine engine :collection collection-name :id document-id})
      (p/upsert-document! backend collection-name document-id data opts)))
  (delete-document! [_ collection-name document-id opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :debug ::delete-document! {:engine engine :collection collection-name :id document-id})
      (p/delete-document! backend collection-name document-id opts)))
  (list-collections [_ opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :debug ::list-collections {:engine engine})
      (p/list-collections backend opts)))

  p/TextSearchQueryProtocol
  (search [_ collection-name query opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :debug ::search {:engine engine :collection collection-name})
      (p/search backend collection-name query opts)))
  (autocomplete [_ collection-name query opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :debug ::autocomplete {:engine engine :collection collection-name})
      (p/autocomplete backend collection-name query opts)))
  (suggest [_ collection-name query opts]
    (let [[engine backend] (delegate engines default-engine opts)]
      (log* logger :debug ::suggest {:engine engine :collection collection-name})
      (p/suggest backend collection-name query opts))))

(defn init-common
  [{:keys [default-engine engines logger]
    :or {default-engine :typesense}}]
  (->CommonTextSearch default-engine engines logger))

