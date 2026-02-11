(ns core-service.app.streams.redis
  (:require [d-core.core.stream.redis.redis]))

;; Compatibility shim:
;; Integrant resolves :core-service.app.streams.redis/backend by loading this namespace.
;; The actual backend implementation lives in d-core.core.stream.redis.redis.
