(ns core-service.app.external.pokemon-api.pokeapi
  (:require [clojure.tools.logging :as log]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(defn json->edn [json-str]
  (json/parse-string json-str true))

(defonce pokemon-cache (atom {}))

(def base-url "https://pokeapi.co/api/v2")

(defn get-pokemon-by-name [http-client cache name]
  (let [url (str base-url "/pokemon/" name)]
    (log/info "Getting pokemon by name" url)
    (println "cache" @cache)
    (println "name" name)
    (println "get from cache" (get @cache name))
    (let [response (or (get @cache name) (http-client/get url))]
      (log/info "Response" (:body response))
      (println "response from" url "->" (:body response))
      (when-not (get @cache name)
        (swap! cache assoc name (json->edn (:body response))))
      response)))

(-> (get-pokemon-by-name http/get pokemon-cache "pikachu") json->edn)
