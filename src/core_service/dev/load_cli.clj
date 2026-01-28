(ns core-service.dev.load-cli
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def conv-id "4e1f25d3-0c72-48c9-ac8a-3dfe03d9275a") ;;sample conversation id for load testing

(defn- parse-long
  [v default]
  (try
    (if (some? v) (Long/parseLong (str v)) default)
    (catch Exception _ default)))

(defn- parse-double
  [v default]
  (try
    (if (some? v) (Double/parseDouble (str v)) default)
    (catch Exception _ default)))

(defn- parse-bool
  [v]
  (let [v (some-> v str/trim str/lower-case)]
    (contains? #{"1" "true" "yes" "y"} v)))

(defn- parse-args
  [args]
  (loop [opts {} args args]
    (if (empty? args)
      opts
      (let [[k v & rest] args]
        (if (and k (str/starts-with? k "--"))
          (recur (assoc opts (keyword (subs k 2)) v) rest)
          (recur (assoc opts :cmd k) (cons v rest)))))))

(defn- base-url
  [opts]
  (or (:base-url opts) "http://localhost:3000"))

(defn- headers
  [opts]
  (cond-> {"accept" "application/json"
           "content-type" "application/json"}
    (:api-key opts) (assoc "x-api-key" (:api-key opts))
    (:token opts) (assoc "authorization" (str "Bearer " (:token opts)))))

(defn- request
  [method url opts body]
  (http/request {:method method
                 :url url
                 :headers (headers opts)
                 :throw-exceptions false
                 :as :text
                 :body (when body (json/generate-string body))}))

(defn- parse-json-body
  [resp]
  (when-let [body (:body resp)]
    (try
      (json/parse-string body true)
      (catch Exception _
        {:raw body}))))

(defn- read-api-keys-file
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (let [data (edn/read-string (slurp file))]
        (cond
          (set? data) data
          (sequential? data) (set data)
          (map? data) (set (or (:keys data) (get data "keys")))
          :else #{})))))

(defn- write-api-keys-file!
  [path keys]
  (spit (io/file path) (pr-str (set keys))))

(defn- random-key
  [length-bytes]
  (let [bytes (byte-array length-bytes)
        rng (java.security.SecureRandom.)]
    (.nextBytes rng bytes)
    (-> (java.util.Base64/getUrlEncoder)
        (.withoutPadding)
        (.encodeToString bytes))))

(defn- resolve-credentials
  [opts]
  (let [user (some-> (:user opts) str/lower-case)
        defaults (cond
                   (#{"2" "user2" "u2"} user) {:username "bob" :password "bob"}
                   :else {:username "alice" :password "alice"})
        username (:username opts)
        password (:password opts)]
    (cond
      (and username password) {:username username :password password}
      username {:username username :password (or password username)}
      :else defaults)))

(defn- decode-base64-url
  [^String s]
  (let [pad (mod (- 4 (mod (count s) 4)) 4)
        padded (str s (apply str (repeat pad "=")))]
    (.decode (java.util.Base64/getUrlDecoder) padded)))

(defn- token-sub
  [token]
  (try
    (let [[_ payload _] (str/split token #"\." 3)
          json-bytes (decode-base64-url payload)
          claims (json/parse-string (String. ^bytes json-bytes "UTF-8") true)]
      (:sub claims))
    (catch Exception _ nil)))

(defn- token-claims
  [token]
  (try
    (let [[_ payload _] (str/split token #"\." 3)
          json-bytes (decode-base64-url payload)]
      (json/parse-string (String. ^bytes json-bytes "UTF-8") true))
    (catch Exception _ nil)))

(defn- token-request
  [{:keys [token-url client-id client-secret]} params]
  (let [params (cond-> params
                 client-id (assoc :client_id client-id)
                 client-secret (assoc :client_secret client-secret))
        resp (http/post token-url {:form-params params
                                   :as :text
                                   :throw-exceptions false})
        status (:status resp)
        parsed (parse-json-body resp)]
    (if (<= 200 status 299)
      {:ok true :data parsed}
      {:ok false :status status :error parsed})))

(defn- keycloak-token!
  [opts]
  (let [{:keys [username password]} (resolve-credentials opts)
        keycloak-url (or (:keycloak-url opts) "http://localhost:8080")
        realm (or (:realm opts) "d-core")
        token-url (str keycloak-url "/realms/" realm "/protocol/openid-connect/token")
        client-id (or (:client-id opts) "d-core-api")
        client-secret (:client-secret opts)
        params (cond-> {:grant_type "password"
                        :username username
                        :password password}
                 (seq (:scope opts)) (assoc :scope (:scope opts)))
        resp (token-request {:token-url token-url
                             :client-id client-id
                             :client-secret client-secret}
                            params)]
    (if (:ok resp)
      (get-in resp [:data :access_token])
      (throw (ex-info "keycloak token failed" (select-keys resp [:status :error]))))))

(defn login!
  [opts]
  (if (= "true" (some-> (:direct opts) str/lower-case))
    (keycloak-token! opts)
    (let [{:keys [username password]} (resolve-credentials opts)
          payload (cond-> {:username username
                           :password password}
                    (seq (:scope opts)) (assoc :scope (:scope opts)))
          resp (request :post (str (base-url opts) "/v1/auth/login") opts payload)
          body (parse-json-body resp)]
      (if (and (= 200 (:status resp)) (get-in body [:token :access_token]))
        (get-in body [:token :access_token])
        (throw (ex-info "login failed"
                        {:status (:status resp)
                         :headers (:headers resp)
                         :body body})))))) 

(defn register!
  [opts]
  (let [resp (request :post (str (base-url opts) "/v1/auth/register") opts
                      {:username (:username opts)
                       :password (:password opts)
                       :email (:email opts)
                       :first_name (:first-name opts)
                       :last_name (:last-name opts)})
        body (parse-json-body resp)]
    (if (= 200 (:status resp))
      body
      (throw (ex-info "register failed" {:status (:status resp) :body body})))))

(defn ensure-token
  [opts]
  (or (:token opts)
      (login! opts)))

(defn create-conversation!
  [opts]
  (let [token (ensure-token opts)
        member-ids (or (some-> (:member-ids opts)
                               (str/split #",")
                               (->> (remove str/blank?) vec))
                       (some-> token token-sub vector))
        resp (request :post (str (base-url opts) "/v1/conversations")
                      (assoc opts :token token)
                      {:type "direct"
                       :title (or (:title opts) "Load Test")
                       :member_ids member-ids})
        body (parse-json-body resp)]
    (if (= 200 (:status resp))
      body
      (throw (ex-info "create conversation failed" {:status (:status resp) :body body})))))

(defn send-message!
  [opts conversation-id text]
  (let [token (ensure-token opts)
        resp (request :post
                      (str (base-url opts) "/v1/conversations/" conversation-id "/messages")
                      (assoc opts :token token)
                      {:type "text"
                       :body {:text text}})
        body (parse-json-body resp)]
    (if (= 200 (:status resp))
      body
      (throw (ex-info "send message failed" {:status (:status resp) :body body})))))

(def ^:private sample-words
  ["hello" "chat" "message" "ping" "quick" "brown" "fox" "jumps" "over" "lazy"
   "dog" "stream" "segment" "flush" "redis" "minio" "latency" "metrics" "core"
   "service" "load" "test" "alpha" "beta" "gamma" "delta" "epsilon" "zeta"])

(def ^:private sample-emojis
  ["😀" "😅" "🚀" "🎉" "🔥" "👍" "💬" "❤️" "🧠" "✅"])

(defn- random-text
  [rng target-len emoji-rate]
  (let [sb (StringBuilder.)]
    (loop []
      (when (< (.length sb) target-len)
        (let [word (nth sample-words (.nextInt rng (count sample-words)))]
          (when (pos? (.length sb))
            (.append sb " "))
          (.append sb word)
          (when (and (pos? emoji-rate) (< (.nextDouble rng) emoji-rate))
            (.append sb " ")
            (.append sb (nth sample-emojis (.nextInt rng (count sample-emojis))))))
        (recur)))
    (.toString sb)))

(defn- build-message
  [opts rng i]
  (let [variable? (or (parse-bool (:variable opts))
                      (some? (:min-len opts))
                      (some? (:max-len opts))
                      (parse-bool (:emoji opts))
                      (some? (:emoji-rate opts)))]
    (if variable?
      (let [min-len (parse-long (:min-len opts) 20)
            max-len (parse-long (:max-len opts) 140)
            [min-len max-len] (if (> min-len max-len) [max-len min-len] [min-len max-len])
            span (max 1 (inc (- max-len min-len)))
            target (+ min-len (.nextInt rng span))
            emoji-rate (parse-double (:emoji-rate opts)
                                     (if (parse-bool (:emoji opts)) 0.1 0.0))]
        (random-text rng target emoji-rate))
      (str (or (:text opts) "hello") " #" (inc i)))))

(defn run-send-loop!
  [opts]
  (let [conversation-id (or (:conversation-id opts)
                            (get (create-conversation! opts) :conversation_id))
        count (parse-long (:count opts) 100)
        delay-ms (parse-long (:delay-ms opts) 0)
        rng (java.util.Random.)
        start (System/currentTimeMillis)]
    (dotimes [i count]
      (send-message! opts conversation-id (build-message opts rng i))
      (when (pos? delay-ms)
        (Thread/sleep delay-ms)))
    {:ok true
     :conversation-id conversation-id
     :sent count
     :elapsed-ms (- (System/currentTimeMillis) start)}))

(defn run-rate-loop!
  [opts]
  (let [conversation-id (or (:conversation-id opts)
                            (get (create-conversation! opts) :conversation_id))
        rate (parse-double (:rate opts) 5.0)
        duration-s (parse-long (:duration-s opts) 60)
        rng (java.util.Random.)
        interval-ms (long (max 1 (Math/round (/ 1000.0 rate))))
        end-at (+ (System/currentTimeMillis) (* 1000 duration-s))]
    (loop [i 0]
      (when (< (System/currentTimeMillis) end-at)
        (send-message! opts conversation-id (build-message opts rng i))
        (Thread/sleep interval-ms)
        (recur (inc i))))
    {:ok true
     :conversation-id conversation-id
     :rate rate
     :duration-s duration-s}))

(defn usage
  []
  (str/join
   "\n"
   ["Usage:"
    "  clojure -M -m core-service.dev.load-cli <cmd> [options]"
    ""
    "Commands:"
    "  register           Register a user in Keycloak"
    "  login              Print access token"
    "  token              Alias for login"
    "  whoami             Print decoded token claims"
    "  create-conversation"
    "  send               Send N messages with optional delay"
    "  load               Send messages at a fixed rate for duration"
    "  api-key            Generate an API key (optionally write to .api_keys.edn)"
    ""
    "Common options:"
    "  --base-url http://localhost:3000"
    "  --api-key <key>"
    "  --token <jwt>"
    "  --username <user> --password <pass>"
    "  --user 1|2 (default 1 -> alice/alice, 2 -> bob/bob)"
    "  --direct true (use Keycloak token endpoint directly)"
    "  --keycloak-url http://localhost:8080 --realm d-core --client-id d-core-api"
    ""
    "Send options:"
    "  --conversation-id <uuid>"
    "  --count 100"
    "  --delay-ms 0"
    "  --text \"hello\""
    "  --variable true"
    "  --min-len 20 --max-len 140"
    "  --emoji true --emoji-rate 0.1"
    ""
    "Load options:"
    "  --conversation-id <uuid>"
    "  --rate 5"
    "  --duration-s 60"
    "  --text \"hello\""
    "  --variable true"
    "  --min-len 20 --max-len 140"
    "  --emoji true --emoji-rate 0.1"
    ""
    "Create conversation options:"
    "  --member-ids \"uuid1,uuid2\""
    "  --title \"Load Test\""
    ""
    "Register options:"
    "  --email <email> --first-name <name> --last-name <name>"
    ""
    "API key options:"
    "  --api-keys-file .api_keys.edn"
    "  --write true"]))

(defn -main
  [& args]
  (let [{:keys [cmd] :as opts} (parse-args args)]
    (try
      (case cmd
        "register" (do (register! opts) (println "ok"))
      "login" (println (or (:token opts) (login! opts)))
      "token" (println (or (:token opts) (login! opts)))
      "whoami" (let [token (or (:token opts) (login! opts))
                     claims (token-claims token)]
                 (println (json/generate-string (or claims {:error "invalid token"}))))
        "create-conversation" (println (json/generate-string (create-conversation! opts)))
        "send" (println (json/generate-string (run-send-loop! opts)))
        "load" (println (json/generate-string (run-rate-loop! opts)))
        "api-key" (let [file (or (:api-keys-file opts) ".api_keys.edn")
                        write? (some-> (:write opts) (str/lower-case) (= "true"))
                        length (parse-long (:length opts) 24)
                        key (random-key (int length))
                        existing (or (read-api-keys-file file) #{})
                        updated (conj existing key)]
                    (when write?
                      (write-api-keys-file! file updated))
                    (println key))
        (do
          (println (usage))
          (System/exit 1)))
      (catch clojure.lang.ExceptionInfo e
        (binding [*out* *err*]
          (println (.getMessage e))
          (when-let [data (ex-data e)]
            (println (pr-str data))))
        (System/exit 2)))))
