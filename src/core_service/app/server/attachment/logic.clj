(ns core-service.app.server.attachment.logic
  (:require [clojure.string :as str])
  (:import (java.io ByteArrayInputStream)
           (java.security MessageDigest)
           (java.util UUID)
           (javax.imageio ImageIO)
           (javax.sound.sampled AudioSystem)))

(def ^:private supported-versions #{"original" "alt" "aac" "mp3"})

(defn- coerce-kind
  [kind]
  (when kind
    (let [k (-> (name kind) str/lower-case keyword)]
      (when (contains? #{:image :voice :file} k)
        k))))

(defn- infer-kind
  [content-type]
  (cond
    (and content-type (str/starts-with? content-type "image/")) :image
    (and content-type (str/starts-with? content-type "audio/")) :voice
    :else :file))

(defn resolve-kind
  [kind content-type]
  (or (coerce-kind kind) (infer-kind content-type)))

(defn image-content-type?
  [content-type]
  (and (string? content-type) (str/starts-with? content-type "image/")))

(defn voice-content-type?
  [content-type]
  (and (string? content-type) (str/starts-with? content-type "audio/")))

(defn- content-type->ext
  [content-type]
  (case content-type
    "image/jpeg" "jpg"
    "image/jpg" "jpg"
    "image/png" "png"
    "image/gif" "gif"
    "image/webp" "webp"
    "audio/mpeg" "mp3"
    "audio/mp3" "mp3"
    "audio/ogg" "ogg"
    "audio/webm" "webm"
    "audio/wav" "wav"
    "audio/x-wav" "wav"
    "audio/aac" "aac"
    "audio/mp4" "m4a"
    "bin"))

(defn- build-object-key
  [{:keys [prefix kind ext]}]
  (let [prefix (or prefix "attachments/")
        kind (or kind :file)
        ext (or ext "bin")]
    (str prefix (name kind) "/" (UUID/randomUUID) "." ext)))

(defn derive-alt-key
  [object-key]
  (when object-key
    (let [base (str/replace (str object-key) #"\.[^./]+$" "")]
      (str base "-alt.jpg"))))

(defn derive-aac-key
  [object-key]
  (when object-key
    (let [base (str/replace (str object-key) #"\.[^./]+$" "")]
      (str base "-aac.m4a"))))

(defn derive-mp3-key
  [object-key]
  (when object-key
    (let [base (str/replace (str object-key) #"\.[^./]+$" "")]
      (str base "-mp3.mp3"))))

(defn derive-variant-key
  [object-key version]
  (case (some-> version str/lower-case)
    "alt" (derive-alt-key object-key)
    "aac" (derive-aac-key object-key)
    "mp3" (derive-mp3-key object-key)
    object-key))

(defn supports-version?
  [version]
  (contains? supported-versions (or version "original")))

(defn- original-matches-target?
  [version mime-type]
  (let [mime (some-> mime-type str/lower-case)]
    (case version
      "aac" (= mime "audio/mp4")
      "mp3" (or (= mime "audio/mpeg") (= mime "audio/mp3"))
      false)))

(defn resolve-attachment-variant
  [attachment-row version]
  (let [version (or (some-> version str/lower-case) "original")
        object-key (:object_key attachment-row)
        mime-type (or (:mime_type attachment-row) "application/octet-stream")]
    (cond
      (not (supports-version? version))
      {:status :invalid-version}

      (= version "original")
      {:status :ok
       :version "original"
       :object-key object-key
       :content-type mime-type}

      (= version "alt")
      (if (image-content-type? mime-type)
        {:status :ok
         :version "alt"
         :object-key (derive-alt-key object-key)
         :content-type "image/jpeg"}
        {:status :incompatible-version})

      (or (= version "aac") (= version "mp3"))
      (if (voice-content-type? mime-type)
        (if (original-matches-target? version mime-type)
          {:status :ok
           :version version
           :object-key object-key
           :content-type (if (= version "aac") "audio/mp4" "audio/mpeg")
           :aliased-original? true}
          {:status :ok
           :version version
           :object-key (derive-variant-key object-key version)
           :content-type (if (= version "aac") "audio/mp4" "audio/mpeg")})
        {:status :incompatible-version})

      :else
      {:status :invalid-version})))

(defn variant-keys-for-mime
  [object-key mime-type]
  (cond-> []
    (image-content-type? mime-type) (conj (derive-alt-key object-key))
    (voice-content-type? mime-type) (into [(derive-aac-key object-key)
                                           (derive-mp3-key object-key)])))

(defn- sha256-hex
  [^bytes bytes]
  (let [digest (MessageDigest/getInstance "SHA-256")
        hash-bytes (.digest digest bytes)]
    (apply str (map (fn [b] (format "%02x" (bit-and b 0xff))) hash-bytes))))

(defn- image-dimensions
  [^bytes bytes]
  (try
    (let [image (ImageIO/read (ByteArrayInputStream. bytes))]
      (when image
        {:width (.getWidth image)
         :height (.getHeight image)}))
    (catch Exception _
      nil)))

(defn- voice-duration-ms
  [^bytes bytes]
  (try
    (with-open [stream (AudioSystem/getAudioInputStream (ByteArrayInputStream. bytes))]
      (let [format (.getFormat stream)
            frames (.getFrameLength stream)
            frame-rate (.getFrameRate format)]
        (when (and (pos? frames) (pos? frame-rate))
          (long (* 1000.0 (/ frames frame-rate))))))
    (catch Exception _
      nil)))

(defn prepare-attachment
  [{:keys [bytes content-type filename kind attachments-prefix source
           expected-size-bytes expected-mime-type]}]
  (let [content-type (or expected-mime-type content-type "application/octet-stream")
        kind (resolve-kind kind content-type)
        ext (content-type->ext content-type)
        object-key (build-object-key {:prefix attachments-prefix
                                       :kind kind
                                       :ext ext})
        size-bytes (long (or expected-size-bytes (alength ^bytes bytes)))
        checksum (sha256-hex bytes)
        image (when (= kind :image) (image-dimensions bytes))
        voice-duration (when (= kind :voice) (voice-duration-ms bytes))]
    (cond
      (and (= kind :image) (nil? image))
      {:ok false :error "invalid image payload"}

      :else
      (let [base {:attachment_id (UUID/randomUUID)
                  :object_key object-key
                  :mime_type content-type
                  :size_bytes size-bytes
                  :checksum checksum
                  :meta {:filename filename
                         :kind kind
                         :upload_source source}}
            attachment (cond-> base
                         image (assoc :image image)
                         voice-duration (assoc :voice {:duration_ms voice-duration}))]
        {:ok true
         :object-key object-key
         :attachment attachment}))))
