(ns core-service.messaging.codecs
  ;; Integrant requires `core-service.messaging.codecs` for keys like
  ;; :core-service.messaging.codecs/edn and :core-service.messaging.codecs/json.
  ;; This namespace exists to load the concrete codec implementations.
  (:require [core-service.messaging.codecs.edn]
            [core-service.messaging.codecs.json]))

