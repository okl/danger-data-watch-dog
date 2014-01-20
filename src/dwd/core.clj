(ns dwd.core
  "Creating a small DSL to run queries and report
back whether they return 1 or 0"
  {:author "Alex Bahouth"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j])
  (:require [diesel.core :refer :all]
            [roxxi.utils.print :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; helpers
(defn- joins
  "Join with spaces"
  [& strs]
  (join " " strs))

(defn- append-desc [env-map desc]
  (update-in env-map [:desc] #(joins % desc)))

(defn- config-db [env-map conn-info]
  (assoc env-map :db-conn-info conn-info))


(defmacro deforder [id orders]
  `(def ~id (quote ~orders)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

;; TODO make better
(def results (atom []))

(defn- add-result! [val]
  (swap! results #(conj % val)))

(definterpreter exec-interp [env]
  ['testing => :testing]
  ['check => :check]
  ['query => :query]
  ['with-db => :with-db])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## testing
;; ex. `'(testing DESC EXPR+)`
;;
(defmethod exec-interp :testing [[_ desc & exprs] env]
  (let [new-env (append-desc env desc)]
    (map #(exec-interp % new-env) exprs)))



(defrecord CheckResult [id desc val])

(defn- make-check-result [id desc val]
  (CheckResult. id desc val))



(defn- result->pass-fail
  "It's assumed that an vec of vecs is returned
where the first vec contains the vec of column headers (of
which there should only be one) and the second vec is an
vec of values of which there should only be one"
  [result]
  (let [extracted-value (print-expr (first (second (print-expr result))))]
    (cond
     (= extracted-value 1) :pass
     (= extracted-value 0) :fail
    :else
    (do
      ;; log this
      (print-expr (str "Unable to determine whether >>>"
                       result
                       "<<< is a success or failure"))
      :error))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## check
;; ex. `'(check DESC EXPR)`
;;
(defmethod exec-interp :check [[_ desc expr] env]
  (let [new-env (append-desc env desc)
        result (try
                 (exec-interp expr new-env)
                 (catch java.lang.Exception e
                     (print-expr e)))
        pass-fail (result->pass-fail result)
        check-result (make-check-result (or (:id new-env) :anon)
                                        (:desc new-env)
                                        pass-fail)]
    (add-result! check-result )))


(defmethod exec-interp :with-db [[_ conn-info-or-sym expr] env]
  (let [conn-info (if (symbol? conn-info-or-sym)
                    @(resolve conn-info-or-sym)
                    conn-info-or-sym)
        new-env (config-db env conn-info)]
    (exec-interp expr new-env)))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## query
;; ex.
;; ```
;; (with-db DB-CONN
;;   ...
;;   '(query QUERY)+)
;; ```
;;
(defmethod exec-interp :query [[_ sql-query] env]
  (j/query (:db-conn-info env) [sql-query] :as-arrays? true))



(def sql-lite-test-db {:subprotocol "sqlite"
                       :subname "test/db/test.db"})

(defn bootstrap-sqlite []
  (let [e #(j/execute! sql-lite-test-db %)
        q #(j/query sql-lite-test-db %)]
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





;; (exec-interp '(query 5) {})

(print-expr (exec-interp three-test {}))
(print-expr (exec-interp four-test {}))
(print-expr (exec-interp five-test {}))
(print-expr @results)
;;(print-expr (exec-interp four-test {}))
(swap! results empty)
