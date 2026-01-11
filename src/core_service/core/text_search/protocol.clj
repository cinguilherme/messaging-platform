(ns core-service.core.text-search.protocol)

(defprotocol TextSearchAdminProtocol
  (create-collection! [_ collection-name schema opts]
    "Creates a new collection/index.

     - schema: engine-specific map (Typesense schema map, etc)
     - opts: engine-specific options")
  (drop-collection! [_ collection-name opts]
    "Drops a collection/index.")
  (upsert-document! [_ collection-name document-id data opts]
    "Creates or updates a document.

     - data is a Clojure map (engine must translate to its required shape).")
  (delete-document! [_ collection-name document-id opts]
    "Deletes a document by id.")
  (list-collections [_ opts]
    "Lists collections/indexes."))

(defprotocol TextSearchQueryProtocol
  (search [_ collection-name query opts]
    "Searches for documents.

     - query can be a string or a map (engine-specific query params).")
  (autocomplete [_ collection-name query opts]
    "Autocompletes a query (engine-specific).")
  (suggest [_ collection-name query opts]
    "Suggests a query (engine-specific)."))

