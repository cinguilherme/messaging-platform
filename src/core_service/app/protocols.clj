(ns core-service.app.protocols)

(defprotocol ProfileService
  (resolve-user-profile [this user-id] "Fetch from local DB or fallback to Keycloak"))
