(ns dwd.core
  "Creating a small DSL to run queries and report
back whether they return 1 or 0"
  {:author "Alex Bahouth"
   :date "Jan 20, 2014"}
  (:require
   [clojure.string :refer [join]])
  (:require
   [diesel.core :refer :all]
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
  (update-in env-map [:db-conn-info] #(joins % conn-info)))



(defmacro deforder [id orders]
  `(def ~id (quote ~orders)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

;; TODO make better
(def results (atom []))

(definterpreter exec-interp [env]
  ['testing => :testing]
  ['check => :check]
  ['query => :query]
  ['with-db => :with-db])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## testing
;; ex. `'(testing DESC EXPR)`
;;
(defmethod exec-interp :testing [[_ desc expr] env]
  (exec-interp expr (append-desc env desc)))


(defn- make-rec [id desc val]
  {:id id :desc desc :result val})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## check
;; ex. `'(check DESC EXPR)`
;;
(defmethod exec-interp :check [[_ id desc expr] env]
  (let [new-env (append-desc env desc)
        check-result (exec-interp expr new-env)]
    (swap! results #(conj % (make-rec id (:desc new-env) check-result)))))


;; (defmethod exec-interp :with-db [[_ conn-info expr] env]
;;   (binding

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## query
;; ex.
;; ```
;; (with-db DB-CONN
;;   ...
;;   '(query QUERY)+)
;; ```
;;
(defmethod exec-interp :query [[_ sql-query] env] sql-query)



(deforder three-test
  (testing "if it's truth that"
    (check :my-id "this returns 5"
           (query 5))))

(deforder four-test
  (testing "can we check"
    (testing "if it's truth that"
      (check :my-id "this returns 5"
             (query 5)))))





(exec-interp '(query 5) {})
(print-expr @results)
(print-expr (exec-interp three-test {}))
(print-expr (exec-interp four-test {}))
(swap! results empty)
