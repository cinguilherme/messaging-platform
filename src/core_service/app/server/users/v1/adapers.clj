(ns core-service.app.server.users.v1.adapers
  (:require
   [clojure.string :as str]
   [core-service.app.config.webdeps]))

(defn normalize-email
  [value]
  (some-> value str/trim str/lower-case))

(defn normalize-username
  [value]
  (let [value (some-> value str/trim)]
    (when (and value (not (str/blank? value)))
      (if (str/starts-with? value "@")
        (subs value 1)
        value))))

(defn user->item
  [user]
  {:user_id (:id user)
   :email (:email user)
   :username (:username user)
   :first_name (:firstName user)
   :last_name (:lastName user)
   :enabled (:enabled user)})

(defn attribute-value
  [attrs k]
  (let [v (or (get attrs k) (get attrs (keyword (name k))))]
    (cond
      (vector? v) (first v)
      (sequential? v) (first v)
      :else v)))

(defn keycloak-user->profile
  [user]
  (let [attrs (:attributes user)]
    {:user_id (:id user)
     :username (:username user)
     :first_name (:firstName user)
     :last_name (:lastName user)
     :avatar_url (or (attribute-value attrs :avatar_url)
                     (attribute-value attrs :avatarUrl))
     :email (:email user)
     :enabled (:enabled user)}))

(defn profile->item
  [profile]
  (-> profile
      (update :user_id (fn [v] (when v (str v))))
      (select-keys [:user_id :username :first_name :last_name :avatar_url :email :enabled])))

(defn profile->db
  [profile]
  {:user-id (:user_id profile)
   :username (:username profile)
   :first-name (:first_name profile)
   :last-name (:last_name profile)
   :avatar-url (:avatar_url profile)
   :email (:email profile)
   :enabled (:enabled profile)})
