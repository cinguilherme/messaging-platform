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
