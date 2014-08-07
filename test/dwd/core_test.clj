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
    (is (= (result (exec-interp check-test {})) true))))


(deftest date
  (testing "date comparison"
    (is (= true
           (result (exec-interp '(check "blah" (= (today) (today))) {}))))
    (is (= true
           (result (exec-interp '(check "blah" (>= (today) (yesterday))) {}))))
    (is (= true
           (result (exec-interp '(check "blah" (<= (yesterday) (today))) {}))))
    (is (= true
           (result
            (exec-interp
             '(check "blah" (>= (today)
                                (date-minus (today) (days 200))))
             {})))))
  (testing "compare dates to file mtimes"
    (is (= true
           (result
            (exec-interp
             '(check "blah" (>= (file-mtime "project.clj")
                                (date-minus (today) (years 50))))
             {}))))))
