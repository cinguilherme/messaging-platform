(ns core-service.unit.attachment-logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.server.attachment.logic :as logic]))

(deftest derive-alt-key-uses-predictable-suffix
  (testing "replaces extension with -alt.jpg"
    (is (= "attachments/image/abc-alt.jpg"
           (logic/derive-alt-key "attachments/image/abc.png")))
    (is (= "attachments/image/abc-alt.jpg"
           (logic/derive-alt-key "attachments/image/abc.jpg"))))
  (testing "works when object key has no extension"
    (is (= "attachments/image/abc-alt.jpg"
           (logic/derive-alt-key "attachments/image/abc"))))
  (testing "nil object key remains nil"
    (is (nil? (logic/derive-alt-key nil)))))

(deftest derive-audio-variant-keys-use-predictable-suffixes
  (testing "aac and mp3 keys derive from original object key"
    (is (= "attachments/voice/abc-aac.m4a"
           (logic/derive-aac-key "attachments/voice/abc.webm")))
    (is (= "attachments/voice/abc-mp3.mp3"
           (logic/derive-mp3-key "attachments/voice/abc.webm"))))
  (testing "derive-variant-key dispatches correctly"
    (is (= "attachments/voice/abc-aac.m4a"
           (logic/derive-variant-key "attachments/voice/abc.webm" "aac")))
    (is (= "attachments/voice/abc-mp3.mp3"
           (logic/derive-variant-key "attachments/voice/abc.webm" "mp3")))))

(deftest resolve-attachment-variant-matrix
  (let [image-row {:object_key "attachments/image/img.png"
                   :mime_type "image/png"}
        voice-row {:object_key "attachments/voice/v.webm"
                   :mime_type "audio/webm"}
        mp4-voice-row {:object_key "attachments/voice/v.m4a"
                       :mime_type "audio/mp4"}
        file-row {:object_key "attachments/file/f.bin"
                  :mime_type "application/octet-stream"}]
    (testing "image supports original and alt"
      (is (= :ok (:status (logic/resolve-attachment-variant image-row nil))))
      (is (= :ok (:status (logic/resolve-attachment-variant image-row "alt"))))
      (is (= :incompatible-version
             (:status (logic/resolve-attachment-variant image-row "aac")))))
    (testing "voice supports original, aac and mp3"
      (is (= :ok (:status (logic/resolve-attachment-variant voice-row "original"))))
      (is (= :ok (:status (logic/resolve-attachment-variant voice-row "aac"))))
      (is (= :ok (:status (logic/resolve-attachment-variant voice-row "mp3"))))
      (is (= :incompatible-version
             (:status (logic/resolve-attachment-variant voice-row "alt")))))
    (testing "aac version aliases original when mime already matches target"
      (let [resolved (logic/resolve-attachment-variant mp4-voice-row "aac")]
        (is (= :ok (:status resolved)))
        (is (= "attachments/voice/v.m4a" (:object-key resolved)))
        (is (true? (:aliased-original? resolved)))))
    (testing "file supports original only"
      (is (= :ok (:status (logic/resolve-attachment-variant file-row "original"))))
      (is (= :incompatible-version
             (:status (logic/resolve-attachment-variant file-row "mp3")))))
    (testing "unknown version is invalid"
      (is (= :invalid-version
             (:status (logic/resolve-attachment-variant voice-row "wav")))))))
