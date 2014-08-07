(ns dwd.files.core-test
  (:require [clojure.test :refer :all])
  (:require [dwd.files.core :refer :all]
            [dwd.files.local-file :refer [make-local-file]]
            [dwd.files.ftp-file :refer [make-ftp-file]]
            [dwd.files.s3-file :refer [make-s3-file]]
            [dwd.files.sftp-file :refer [make-sftp-file]]
            [dwd.files.file :refer :all]
            [dwd.check-result :refer :all])
  (:import dwd.files.core.ErrorFile))

(deftest file-for-type-test
  (testing "Error file"
    (is (= (instance? ErrorFile ((file-for-type "blah") "" "") true)))
    (is (= (instance? ErrorFile ((file-for-type :blah) "" "") true))))
  (testing "SftpFile"
    (is (= make-sftp-file (file-for-type "sftp")))
    (is (= make-sftp-file (file-for-type :sftp))))
  (testing "FtpFile"
    (is (= make-ftp-file (file-for-type "ftp")))
    (is (= make-ftp-file (file-for-type :ftp))))
  (testing "S3File"
    (is (= make-s3-file (file-for-type "s3")))
    (is (= make-s3-file (file-for-type :s3))))
  (testing "LocalFile"
    (is (= make-local-file (file-for-type "local")))
    (is (= make-local-file (file-for-type :local)))
    (is (= make-local-file (file-for-type nil)))))

(deftest make-error-result-test
  (testing "file-present?"
    (is (= :error
           (result (file-present? ((file-for-type "blah") "" "")))))
    (is (= "Unable to find type for blah")
           (exceptions (file-present? ((file-for-type "blah") "" "")))))
  (testing "file-mtime"
    (is (= :error
           (result (file-mtime ((file-for-type "blah") "" "")))))
    (is (= "Unable to find type for blah")
           (exceptions (file-mtime ((file-for-type "blah") "" "")))))
  (testing "file-hash"
    (is (= :error
           (result (file-hash ((file-for-type "blah") "" "")))))
    (is (= "Unable to find type for blah")
           (exceptions (file-hash ((file-for-type "blah") "" "")))))
  (testing "file-size"
    (is (= :error
           (result (file-size ((file-for-type "blah") "" "")))))
    (is (= "Unable to find type for blah")
           (exceptions (file-size ((file-for-type "blah") "" ""))))))
