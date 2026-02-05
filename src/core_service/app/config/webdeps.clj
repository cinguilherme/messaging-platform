(ns core-service.app.config.webdeps
  (:require [integrant.core :as ig]))

;; WEB deps component is a facility to allow most of the systems in the duct.edn to add in just this dependency and get a bundle of dependencies, such as logger, db, metrics, producer, etc.

(defrecord WebDeps [logger db metrics producer consumer storage
                    token-client keycloak redis naming minio segments
                    idempotency logging receipt])

(defmethod ig/init-key :core-service.app.config.webdeps/webdeps
  [_ deps]
  (map->WebDeps deps))
