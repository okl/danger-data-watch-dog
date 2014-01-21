(ns dwd.core-test
  (:use clojure.test
        dwd.core)
  (:require [clojure.java.jdbc :as j]
            [clojure.string :refer [join]]))



(def sql-lite-test-db {:subprotocol "sqlite"
                       :subname "test/db/test"})

(defn bootstrap-sqlite []
  (let [e #(j/execute! sql-lite-test-db %)
        q #(j/query sql-lite-test-db %)]
    (e ["DROP TABLE IF EXISTS test"])
    (e ["CREATE TABLE IF NOT EXISTS test (name varchar(10), age int)"])
    (e [(join " "
              ["INSERT INTO test SELECT 'Alex' AS name, 30 AS age"
               "UNION SELECT 'Karl', 28"
               "UNION SELECT 'Eric', 29"
               "UNION SELECT 'Egor', 23"])])))


(bootstrap-sqlite)


(deforder three-test
  (testing "if it's truth that"
    (check "this returns 5"
           (with-db sql-lite-test-db
             (query "select CASE WHEN count(*) > 3 THEN 1 ELSE 0 END AS a_test from test")))))

(deforder four-test
  (with-db sql-lite-test-db
    (testing "if it's truth that"
      (check "this returns 5"
             (query "select CASE WHEN count(*) > 3 THEN 1 ELSE 0 END AS a_test from test"))
      (check "this returns 10"
             (query "select CASE WHEN count(*) > 20 THEN 1 ELSE 0 END AS a_test from test")))))

(deforder five-test
  (testing "can we check"
    (with-db sql-lite-test-db
      (testing "if it's truth that"
        (check "this returns 5"
               (query "select 5 from test limit 1"))))))

(deforder five-test
  (testing "can we check"
      (testing "if it's truth that"
        (check "this returns 5"
               (query "select 5 from test limit 1")))))


;; (exec-interp '(query 5) {})

(exec-interp three-test {})
(exec-interp four-test {})
(exec-interp five-test {})
;; @results
;;(print-expr (exec-interp four-test {}))
;;(swap! results empty)
