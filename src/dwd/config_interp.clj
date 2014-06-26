(ns dwd.config-interp
  ""
  {:author "Alex Bahouth & Eric Sayle"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log])
  (:require [diesel.core :refer :all]
            [roxxi.utils.print :refer :all]))


(defn- append-desc [env-map desc]
  (update-in env-map [:desc] #(join % desc)))


(definterpreter config-interp [env]
  ['testing => :testing]
  ['check => :check]
  ;; Supporting Databases
  ['with-db => :with-db]
  ['query => :query]
  ;; Predicates
  ['= => :=])


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## testing
;; ex. `'(testing DESC EXPR+)`
;;
(defmethod config-interp :testing [[_ desc & exprs] env]
  (let [new-env (append-desc env desc)
        nexts (map #(config-interp % new-env) exprs)]
    (str (format "Performing the following checks to verify %s:\n" desc)
         (join "\n" nexts))))





;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## check
;; ex. `'(check DESC EXPR)`
;;
;; this is the configuration of a check
;; => should yield a description of this check
(defmethod config-interp :check[[_ & args] env]
  (let [args (if (= count args 3)
               args
               (cons :anon args))
        id  (first args)
        desc (second args)
        expr (last args)]
    (str "check (with id " id ") " desc " " (config-interp expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## =
;; ex. `'(= =expr1 =expr2 ...)`
(defmethod config-interp := [[_ & =exprs] env]
  (str "that " (join ", " =exprs) " are equal."))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## query
;; ex.
;; ```
;; (with-db DB-CONN
;;   ...
;;   '(query QUERY)+)
;; ```
;;
(defmethod config-interp :query [[_ sql-query] env]
  (format "that %s returns 1" sql-query))
