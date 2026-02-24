(ns core-service.app.pagination
  (:require [d-core.core.pagination.token :as token]))

(defn encode-token
  [token]
  (token/encode-token token))

(defn decode-token
  [token]
  (token/decode-token token))
