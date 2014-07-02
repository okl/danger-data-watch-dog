(ns dwd.core
  "Creating a small DSL to run queries and report
back whether they return 1 or 0"
  {:author "Alex Bahouth"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j]
            [clojure.java.io :refer [as-file]]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]])
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
  (time-executed [this])
  (execution-duration [this])
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
    (time-executed [_]
      (:time config))
    (execution_duration [_]
      (:duration config))
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## lookup-config
;; ex `'(with-s3 (lookup my-s3-config)` ... )
;;
;; This looks up a configuration defined using defconfig or define-config
(defmethod exec-interp :lookup-config [[_ config-def] env]
  (let [registry (get env :config-registry)
        config (get registry config-def)]
    (if (nil? config)
      (log/errorf "Unknown configuration %s specified. Available: %s"
                  config-def (keys registry))
      config)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## define-config
;; ex ```
;;     '(with-s3 (define-config my-s3-config {:access-key "blah"
;;                                            :secret-key "blah"
;;                                            :bucket "blah"}
;;
;; This defines a configuration that can both be used later, and
;; at the present time
(defmethod exec-interp :define-config [[_ id config-map & exprs] env]
  (let [new-env (merge env {:config-registry (defconfig id config-map)})]
    (str "Using configuration with id " id " as " config-map "\n"
         (join "\n" (map #(exec-interp % new-env) exprs)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; databases
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod exec-interp :with-db [[_ conn-info-or-sym expr] env]
  (let [conn-info (if (symbol? conn-info-or-sym)
                    @(resolve conn-info-or-sym)
                    conn-info-or-sym)
        new-env (config-db env conn-info)]
    (exec-interp expr new-env)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-s3
;; ex. `'(with-s3 s3-config (file-present? /path/to/file)`
;;
(defmethod exec-interp :with-s3 [[_ s3-config & exprs] env]
  (let [config (exec-interp s3-config env)
        new-env (merge env {:location "s3" :s3-config config})]
    (map #(exec-interp % new-env) exprs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-ftp
;; ex. `'(with-ftp ftp-config (file-present? /path/to/file)`
;;
(defmethod exec-interp :with-ftp [[_ ftp-config & exprs] env]
  (str "using ftp defined by " (exec-interp ftp-config env ) "\n"
       (join "\n" (map #(exec-interp % env) exprs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; predicates
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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


;; cheater cheater pumpkin eater
(defn local-file-present? [expr env]
  (make-check-result
   { :result (.exists (as-file expr))
     :data expr
     :desc (get env :desc)}))

(defn s3-file-present? [expr env]
  (let [s3-config (:s3-config env)
        secret-key (:secret-key s3-config)
        access-key (:access-key s3-config)]
    (if (or (empty? secret-key)
            (empty? access-key))
      ; bad configuration
      (make-check-result
       { :result :error
         :data expr
        :desc (:desc env)
        :messages "Need secret-key and access-key for s3 config"})
      ; run the s3cmd to see if it's there
      (let [sh-result (sh "/Users/esayle/s3cmd-1.5.0-rc1/s3cmd"
                      (format "--access_key=%s" access-key)
                      (format "--secret_key=%s" secret-key)
                      "ls"
                      expr)
            result (if (and (= 0 (:exit sh-result))
                            (not (empty? (:out sh-result)))
                            (> (.indexOf (:out sh-result) expr) -1))
                     true
                     false)
            messages (:out sh-result)
            exceptions (:err sh-result)
            desc (get env :desc)]
        (make-check-result
         {:result result
          :data expr
          :desc desc
          :messages messages
          :exceptions exceptions})))))

(defn ftp-file-present? [expr env]
  (println "Looking in ftp for " expr))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-present?
;; `'(file-present? /path/to/file.txt)`
;;
(defmethod exec-interp :file-present? [[_ file-expr] env]
  (let [location (:location env)]
    (case location
      nil   (local-file-present? file-expr env)
      "s3"  (s3-file-present? file-expr env)
      "ftp" (ftp-file-present? file-expr env))))

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

