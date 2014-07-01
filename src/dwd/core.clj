(ns dwd.core
  "Creating a small DSL to run queries and report
back whether they return 1 or 0"
  {:author "Alex Bahouth"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log])
  (:require [diesel.core :refer :all]
            [roxxi.utils.print :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; helpers
(defn- joins
  "Join with spaces"
  [& strs]
  (join " " strs))

(defn- join-path
  "Join with dots"
  [ & strs]
  (join "." strs))

(defn- append-desc [env-map desc]
  (update-in env-map [:desc] #(joins % desc)))

(defn- append-id [env-map id]
  (update-in env-map [:id] #(join-path % id)))

(defn- config-db [env-map conn-info]
  (assoc env-map :db-conn-info conn-info))

(defmacro deforder [id orders]
  `(def ~id (quote ~orders)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Check Results
;;
;; In order to have consistency in our return values, let's define what every check
;; should return so we can layer on higher order logic

(defprotocol CheckResult
  (result [this])
  (exceptions [this])
  (execution-time [this])
  (messages [this])
  (data [this])
  (description [this]))


(defn make-check-result [config]
  (reify
    CheckResult
    (result [_]
      (:result config))
    (exceptions [_]
      (:exceptions config))
    (execution-time [_]
      (:time config))
    (messages [_]
      (:messages config))
    (data [_]
      (:data config))
    (description [_]
      (:desc config))))

;; holds configuration symbols
(def config-sym-tab (atom {}))

(defn defconfig [k v]
  (when (contains? @config-sym-tab k)
    (log/warnf "Attempting to overwrite already defined function %s" k))
  (swap! config-sym-tab #(assoc % k v)))

(defn config-registry []
  @config-sym-tab)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

;; TODO make better
(def results (atom []))

(defn- add-result! [val]
  (swap! results #(conj % val)))

(definterpreter exec-interp [env]
  ['testing => :testing]
  ['check => :check]
  ['lookup-config => :lookup-config]
  ['define-config => :define-config]
  ;; Supporting Databases
  ['with-db => :with-db]
  ;; Supporting Files
  ['with-s3 => :with-s3]
  ['with-ftp => :with-ftp]
  ;; Predicates
  ['= => :=]
  ['>= => :>=]
  ['file-present? => :file-present?])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## testing
;; ex. `'(testing DESC EXPR+)`
;;
(defmethod exec-interp :testing [[_ desc & exprs] env]
  (let [new-env (append-desc env desc)]
    (map #(exec-interp % new-env) exprs)))


(defn- result->pass-fail
  "It's assumed that an vec of vecs is returned
where the first vec contains the vec of column headers (of
which there should only be one) and the second vec is an
vec of values of which there should only be one"
  [result]
  (let [extracted-value (first (second result))
        success-fn #(= 1 %)]
    (cond
     (success-fn extracted-value) :pass
     (not (success-fn extracted-value)) :fail
    :else
    (do
      ;; log this
      (log/error
       (format "Unable to determine whether >>>%s<<< is a success or failure"
               result))

      :error))))



 (defn- process-check-args [args]
  (if (= (count args) 3)
    args
    (cons :anon args)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## check
;; ex. `'(check DESC EXPR)`
;;
;; this is the configuration of a check

;; this is the execution of a check
(defmethod exec-interp :check [[_ & args] env]
  (let [[id desc expr] (process-check-args args)
        new-env (append-desc env desc)
        result (try
                 (exec-interp expr new-env)
                 (catch Exception e
                     (log/spy e)))]
    (add-result! result)))


(defmethod exec-interp :with-db [[_ conn-info-or-sym expr] env]
  (let [conn-info (if (symbol? conn-info-or-sym)
                    @(resolve conn-info-or-sym)
                    conn-info-or-sym)
        new-env (config-db env conn-info)]
    (exec-interp expr new-env)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## =
;; ex. `'(= =expr1 =expr2 ...)`
;; execution of a predicate
 (defmethod exec-interp := [[_ & =exprs] env]
   (let [results (map deref (map resolve =exprs))
         result (apply = results)
         result-map {:result (not result)
                     :data results
                     :desc (:desc env)}]
     (make-check-result result-map)))


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
  (let [db-conn (:db-conn-info env)]
    (if (nil? db-conn)
      (throw
       (RuntimeException.
        (str "No Database connection specified. Make sure this `query` block is "
             "inside of a `with-db` block")))
      (j/query (:db-conn-info env) [sql-query] :as-arrays? true))))
