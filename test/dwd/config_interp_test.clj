(ns dwd.config-interp-test
  (:use clojure.test
        dwd.config-interp)
  (:require [clojure.java.jdbc :as j]
            [clojure.string :refer [join]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; setup
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defconfig 'vertica-config
  {:subprotocol "vertica"
   :subname "vertica"
   :classname "com.vertica.driver.VerticaDriver"})

(defconfig 's3-configulation
  {:secret-key "blah"
   :access-key "blah"
   :bucket "blah"})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; testing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest test-simple-interp
  (testing "Verifying that a simple interp works "
    (is (= (config-interp '(check "hello" (= "5" 6)) {})
           "check (with id :anon) hello that 5, 6 are equal."))
    (testing "with id"
      (is (= (config-interp '(check "my-id" "hello" (= "5" 6)) {})
             "check (with id my-id) hello that 5, 6 are equal.")))))


(deftest test-lookup-config
  (testing "Verifying that lookup-config returns the original"
    (is (= (config-interp '(lookup-config vertica-config)
                          {:config-registry (config-registry)})
           {:subprotocol "vertica"
            :subname "vertica"
            :classname "com.vertica.driver.VerticaDriver"}))))

(deftest test-define-config-followed-by-lookup
  (testing "That something defined can be used later"
    (is (= (config-interp '(define-config my-conf {:blah "blah"}
                             (lookup-config my-conf)) {})
           "Using configuration with id my-conf as {:blah \"blah\"}\n{:blah \"blah\"}"))))

(def et-db-test
  '(testing "that the ET database loads work successfully"
    (with-db (lookup-config vertica-config)
      (check "sufficient rows in et.sent"
             (>= 40000
               (query "select count(*) from exact_target.sent where date = CURRENT_DATE - 1"))))))

(deftest test-et-db-interp
  (testing "Verifying db interp works as expected"
    (is (= (config-interp et-db-test {:config-registry (config-registry)})
           "Performing the following checks to verify that the ET database loads work successfully:\nusing database defined by {:subprotocol \"vertica\", :classname \"com.vertica.driver.VerticaDriver\", :subname \"vertica\"}\ncheck (with id :anon) sufficient rows in et.sent that 40000 is greater than (query \"select count(*) from exact_target.sent where date = CURRENT_DATE - 1\")"))))

(def et-db-no-config-registry
  '(testing "that the ET database loads work successfully"
     (with-db {:subprotocol "vertica"
               :subname "vertica"
               :classname "com.vertica.driver.VerticaDriver"}
       (check "sufficient rows in et.sent"
              (>= 40000
                  (query "select count(*) from exact_target.sent where date = CURRENT_DATE - 1"))))))

(deftest test-et-db-no-config-registry
  (testing "Verifying db interp works w/o the config registry"
    (is (= (config-interp et-db-no-config-registry {})
           "Performing the following checks to verify that the ET database loads work successfully:\nusing database defined by {:subprotocol \"vertica\", :subname \"vertica\", :classname \"com.vertica.driver.VerticaDriver\"}\ncheck (with id :anon) sufficient rows in et.sent that 40000 is greater than (query \"select count(*) from exact_target.sent where date = CURRENT_DATE - 1\")"))))

(def arbitrary-command-test
  '(testing "that something local happens okay"
    (check dude-this-is-my-name "Hey I can run arbitrary shell commands"
           (= (shell-command "This is the same as the random command above")
              0))))

(deftest test-arbitrary-command-interp
  (is (= (config-interp arbitrary-command-test {})
         "Performing the following checks to verify that something local happens okay:\ncheck (with id dude-this-is-my-name) Hey I can run arbitrary shell commands that (shell-command \"This is the same as the random command above\"), 0 are equal.")))


(def hash-comparison-test
  '(check "hash of file matches on two environments"
         (= (hash-of-file "/path/to/file")
            (with-stfp [stfp-creds] (hash-of-file "/path/to/file")))))

(deftest test-hash-comparison-interp
  (is (= (config-interp hash-comparison-test {})
         "check (with id :anon) hash of file matches on two environments that (hash-of-file \"/path/to/file\"), (with-stfp [stfp-creds] (hash-of-file \"/path/to/file\")) are equal.")))

(def et-test
  '(testing "ensuring that ET uploads are successful"
     (check local-et-nfs-check "file is present locally"
            (file-present? "///path/to/file/{yyyy}/{MM}/{dd}/file.txt"))
     (with-s3 (lookup-config s3-configulation)
       (check "Has a new file been uploaded"
              (file-present? "s3://path/to/file/{yyyy}/{MM}/{dd}/file.txt"))
       (check "is of a minimum size"
              (>= (file-size "s3://path/to/file/{yyyy}/{MM}/{dd}/file.txt"
                             9123214)))
       (check "has been updated recently"
              (>= (file-mtime "s3://path/to/file/{yyyy}{MM}/{dd}/file.txt" )
                  (date yesterday)))
       (check "random command"
              (= 0
                 (execute-command-rc "s3cmd do something we haven't thought of"))))))

(deftest test-et-interp
  (testing "Verifying ET file interp works as expected"
    (is (= (config-interp et-test {:config-registry (config-registry)})
           "Performing the following checks to verify ensuring that ET uploads are successful:\ncheck (with id local-et-nfs-check) file is present locally that file ///path/to/file/{yyyy}/{MM}/{dd}/file.txt exists\nusing s3 defined by {:secret-key \"blah\", :bucket \"blah\", :access-key \"blah\"}\ncheck (with id :anon) Has a new file been uploaded that file s3://path/to/file/{yyyy}/{MM}/{dd}/file.txt exists\ncheck (with id :anon) is of a minimum size that (file-size \"s3://path/to/file/{yyyy}/{MM}/{dd}/file.txt\" 9123214) is greater than \ncheck (with id :anon) has been updated recently that (file-mtime \"s3://path/to/file/{yyyy}{MM}/{dd}/file.txt\") is greater than (date yesterday)\ncheck (with id :anon) random command that 0, (execute-command-rc \"s3cmd do something we haven't thought of\") are equal."))))
