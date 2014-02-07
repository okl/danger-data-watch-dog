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
  ;; Supporting Databases
  ['with-db => :with-db]
  ['query => :query])

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

  (let [extracted-value (first (second result))]
    (cond
     (= extracted-value 1) :pass
     (= extracted-value 0) :fail
    :else
    (do
      ;; log this
      (log/error
       (format "Unable to determine whether >>>%s<<< is a success or failure"
               result))

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
                     (log/spy e)))
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
  (let [db-conn (:db-conn-info env)]
    (if (nil? db-conn)
      (throw
       (RuntimeException.
        (str "No Database connection specified. Make sure this `query` block is "
             "inside of a `with-db` block")))
      (j/query (:db-conn-info env) [sql-query] :as-arrays? true))))
