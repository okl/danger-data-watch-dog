(ns dwd.core-test
 (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.string :refer [join]])
  (:require [dwd.core :refer :all]
            [dwd.check-result :refer :all]
            [dwd.ops :refer :all]))

(def file-present-test
  '(file-present? "project.clj"))

(def file-not-present-test
  '(file-present? "alskdjflksdjflsdkjf.txt"))

(deftest file-present
  (testing "positive result"
    (is (= (result (exec-interp file-present-test {}))
           true)))
  (testing "negative result"
    (is (= (result (exec-interp file-not-present-test {}))
           false))))

(def check-test
  '(check "Does check work"
          (= 3
             3)))

(deftest check
  (testing "digit equality"
    (is (= (result (last (exec-interp check-test {}))) true))))

(def valid-echo-test
  '(check "3 = 3"
          (= (exec-op shell-output "echo" "3")
             "3")))

(deftest valid-echo
  (testing "expected output"
    (is (= (result (last (exec-interp valid-echo-test {:op-registry (op-registry)})))
           true))))

(def valid-ls-test
  '(testing "Can run ls on my own file"
     (check "file exists"
            (= (exec-op shell-exit-code "ls" "project.clj")
               0))))

(deftest valid-ls
  (let [check (exec-interp valid-ls-test {:op-registry (op-registry)})]
    (testing "expected return code"
      (is (= (result (last (last check))) true)))
    (testing "output is expected"
      (is (= (messages (first (data (last (last check))))) "project.clj")))))
