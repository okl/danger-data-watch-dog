(ns dwd.files.local-file-test
  (:require [clojure.java.io :refer [as-file]]
            [clojure.test :refer :all])
  (:require [dwd.files.local-file :refer :all]
            [dwd.files.file :refer :all]
            [dwd.check-result :refer :all]))

(def test-file
  (make-local-file "test/dwd/files/test.txt" {}))

(def not-real-file
  (make-local-file "blaldjf" {}))

(deftest local-file-test
  (testing "file-present?"
    (is (= true (result (file-present? test-file))))
    (is (= false (result (file-present? not-real-file)))))
  (testing "file-mtime"
    (is (= (.lastModified (as-file "test/dwd/files/test.txt"))
           (result (file-mtime test-file)))))
  (testing "file-hash"
    (is (= "99f16f369162d3c718bcd01644666ee2"
           (result (file-hash test-file)))))
  (testing "file-size"
    (is (= 45
           (result (file-size test-file))))))
