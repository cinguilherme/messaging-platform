(ns core-service.text-search.typesense
  (:require [integrant.core :as ig]
            [core-service.clients.typesense.client :as tc]
            [core-service.text-search.protocol :as p]))

(defrecord TypesenseEngine [client]
  p/TextSearchAdminProtocol
  (list-collections [_ opts]
    (tc/get! client "/collections" opts))

  (create-collection! [_ collection-name schema opts]
    ;; Typesense needs schema to include :name; accept either full schema or schema without name.
    (let [schema' (cond
                    (and (map? schema) (contains? schema :name)) schema
                    (map? schema) (assoc schema :name (str collection-name))
                    :else (throw (ex-info "schema must be a map" {:schema schema})))
          resp (tc/post! client "/collections" (merge opts {:body schema'}))]
      (if (or (<= 200 (:status resp) 299) (= 409 (:status resp)))
        resp
        (throw (ex-info "Typesense create collection failed" {:collection collection-name :resp resp})))))

  (drop-collection! [_ collection-name opts]
    (let [resp (tc/delete! client (str "/collections/" collection-name) opts)]
      (if (or (<= 200 (:status resp) 299) (= 404 (:status resp)))
        resp
        (throw (ex-info "Typesense drop collection failed" {:collection collection-name :resp resp})))))

  (upsert-document! [_ collection-name document-id data opts]
    (when-not (map? data)
      (throw (ex-info "data must be a map" {:data data})))
    ;; Typesense document ids are strings. Put it in the document as :id.
    (let [doc (assoc data :id (str document-id))
          resp (tc/post! client
                         (str "/collections/" collection-name "/documents")
                         (merge opts {:query {:action "upsert"}
                                      :body doc}))]
      (if (<= 200 (:status resp) 299)
        resp
        (throw (ex-info "Typesense upsert document failed" {:collection collection-name :id document-id :resp resp})))))

  (delete-document! [_ collection-name document-id opts]
    (let [resp (tc/delete! client
                           (str "/collections/" collection-name "/documents/" (str document-id))
                           opts)]
      (if (or (<= 200 (:status resp) 299) (= 404 (:status resp)))
        resp
        (throw (ex-info "Typesense delete document failed" {:collection collection-name :id document-id :resp resp})))))

  p/TextSearchQueryProtocol
  (search [_ collection-name query opts]
    (let [q (cond
              (string? query) {:q query}
              (map? query) query
              :else (throw (ex-info "query must be string or map" {:query query})))
          resp (tc/get! client
                        (str "/collections/" collection-name "/documents/search")
                        (merge opts {:query q}))]
      (if (<= 200 (:status resp) 299)
        resp
        (throw (ex-info "Typesense search failed" {:collection collection-name :resp resp})))))

  (autocomplete [this collection-name query opts]
    ;; Typesense doesn't have a dedicated autocomplete endpoint; treat as search with per_page=5.
    (p/search this collection-name (merge {:q query :per_page 5} (when (map? query) query)) opts))

  (suggest [this collection-name query opts]
    ;; Treat suggest as search; engines can override later.
    (p/search this collection-name (merge {:q query :per_page 5} (when (map? query) query)) opts)))

(defmethod ig/init-key :core-service.text-search.typesense/engine
  [_ {:keys [typesense-client] :as opts}]
  (when-not typesense-client
    (throw (ex-info "Typesense engine requires :typesense-client" {:opts opts})))
  (->TypesenseEngine typesense-client))

