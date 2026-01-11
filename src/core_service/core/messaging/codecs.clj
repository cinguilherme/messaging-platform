(ns core-service.core.messaging.codecs
  ;; Integrant requires `core-service.core.messaging.codecs` for keys like
  ;; :core-service.core.messaging.codecs/edn and :core-service.core.messaging.codecs/json.
  ;; This namespace exists to load the concrete codec implementations.
  (:require [core-service.core.messaging.codecs.edn]
            [core-service.core.messaging.codecs.json]))

