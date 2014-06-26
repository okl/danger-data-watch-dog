(ns dwd.config-interp-test
  (:use clojure.test
        dwd.config-interp)
  (:require [clojure.java.jdbc :as j]
            [clojure.string :refer [join]]))

(deftest test-simple-interp
  (testing "Verifying that a simple interp works "
    (is (= (config-interp '(check "hello" (= "5" 6)) {})
           "check (with id :anon) hello that 5, 6 are equal."))))
