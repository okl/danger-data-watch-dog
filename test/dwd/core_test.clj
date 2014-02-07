(ns dwd.core-test
  (:use clojure.test
        dwd.core)
  (:require [clojure.java.jdbc :as j]
            [clojure.string :refer [join]]))



(def sql-lite-test-db {:subprotocol "sqlite"
                       :subname "test/db/person"})

(defn bootstrap-sqlite []
  (let [e #(j/execute! sql-lite-test-db %)
        q #(j/query sql-lite-test-db %)]
    (e ["DROP TABLE IF EXISTS person"])
    (e ["CREATE TABLE IF NOT EXISTS person (name varchar(10), age int)"])
    (e [(join " "
              ["INSERT INTO person SELECT 'Alex' AS name, 30 AS age"
               "UNION SELECT 'Karl', 28"
               "UNION SELECT 'Eric', 29"
               "UNION SELECT 'Egor', 23"])])))


(bootstrap-sqlite)

;; Notes from talking to Erik the consultant
;; It'd be interesting to have an observe function
;; where we record observations about the system
;; data integrity
;; files sizes, hashes, line counts, etc
;; where we're not saying whether or not something
;; is true, but just recording information about
;; files.
;; There are quality checks we may want to perform on files
;; and other observations we may want to record (how many unique values, etc)
;; We may also want to load files into staging tables, then
;; run these quality checks againt them and report back
;; whether or not the data has good integrity with
;; existing data sources before we include them
;; e.g. do all the ids we have exist, are there rows
;; missing ids in a table we would want to join to,
;; etc.


(deforder three-test
  (testing "if it's truth that"
    (check "this returns 5"
           (with-db sql-lite-test-db
             (query "select CASE WHEN count(*) > 3 THEN 1 ELSE 0 END AS a_test from person")))))

(deforder four-test
  (with-db sql-lite-test-db
    (testing "if it's truth that"
      (check "this returns 5"
             (query "select CASE WHEN count(*) > 3 THEN 1 ELSE 0 END AS a_test from person"))
      (check "this returns 10"
             (query "select CASE WHEN count(*) > 20 THEN 1 ELSE 0 END AS a_test from person")))))

(deforder five-test
  (testing "can we check"
    (with-db sql-lite-test-db
      (testing "if it's truth that"
        (check "this returns 5"
               (query "select 5 from person limit 1"))))))

(deforder six-test
  (testing "can we check"
      (testing "if it's truth that"
        (check "this returns 5"
               (query "select 5 from person limit 1")))))
(with-db
  {:subprotocol "sqlite"
   :subname "test/db/person"}
  (group :yoda
         (testing "A bunch of things"
           (group :yesterday
                  (check "If yoda was loaded yesterday" :total
                         (query "as much"))
                  (testing "If each server has it's data: "
                    (check "sac-prod-web-01"
                           (query "select count(*) from yoda.yoda where server_name = sac-prod-web-01"))
                    (check "sac-prod-web-02"
                           (query "select count(*) from yoda.yoda where server_name = sac-prod-web-02"))
                    (check "sac-prod-web-03"
                           (query "select count(*) from yoda.yoda where server_name = sac-prod-web-03")))))))



;; (exec-interp '(query 5) {})

(exec-interp three-test {})
(exec-interp four-test {})
(exec-interp five-test {})
;; @results
;;(print-expr (exec-interp four-test {}))
;;(swap! results empty)
