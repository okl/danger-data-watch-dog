(ns dwd.core
  "How can we check current state of things"
  {:author "Alex Bahouth"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]])
  (:require [diesel.core :refer :all]
            [roxxi.utils.print :refer :all])
  (:require [dwd.files.file :refer :all]
            [dwd.files.core :refer [file-for-type]]
            [dwd.check-result :refer :all]))


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

;; holds configuration symbols
(def config-sym-tab (atom {}))

(defn defconfig [k v]
  (when (contains? @config-sym-tab k)
    (log/warnf "Attempting to overwrite already defined configuration %s" k))
  (swap! config-sym-tab #(assoc % k v)))

(defn config-registry []
  @config-sym-tab)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # The language

(definterpreter exec-interp [env]
  ['testing => :testing]
  ['check => :check]
  ['lookup-config => :lookup-config]
  ['define-config => :define-config]
  ;; Supporting Databases
  ['with-db => :with-db]
  ['query => :query]
  ;; Supporting Files
  ['with-s3 => :with-s3]
  ['with-ftp => :with-ftp]
  ['with-sftp => :with-sftp]
  ;; Predicates
  ['= => :=]
  ['>= => :>=]
  ['file-present? => :file-present?]
  ;; groups
  ['group => :group])

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
    result))

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
;;     '(with-s3 (define-config {:access-key "blah"
;;                               :secret-key "blah"
;;                               :bucket "blah"})
;;
;; This defines a configuration that can both be used later, and
;; at the present time
(defmethod exec-interp :define-config [[_ id config-map & exprs] env]
  config-map)

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
  (let [config (exec-interp ftp-config env)
        new-env (merge env {:location "ftp" :ftp-config config})]
    (map #(exec-interp % new-env) exprs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-sftp
;; ex. `'(with-sftp sftp-config (file-present? /path/to/file)`
;;
(defmethod exec-interp :with-sftp [[_ sftp-config & exprs] env]
  (let [config (exec-interp sftp-config env)
        new-env (merge env {:location "sftp" :sftp-config config})]
    (map #(exec-interp % new-env) exprs)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; predicates
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## =
;; ex. `'(= =expr1 =expr2 ...)`
;; execution of a predicate
(defmethod exec-interp := [[_ & =exprs] env]
  (let [results (map #(if (or (symbol? %) (seq? %))
                        (exec-interp % env)
                        %)
                     =exprs)
        result (apply = (map #(if (satisfies? CheckResult %)
                                (.result %)
                                %)
                             results))
        result-map {:result result
                    :data results
                    :desc (:desc env)}]
    (make-check-result result-map)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-present?
;; `'(file-present? /path/to/file.txt)`
;;
(defmethod exec-interp :file-present? [[_ file-expr] env]
  (let [location (:location env)
        location (if (nil? location) :local location)]
    (file-present? ((file-for-type location) file-expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-mtime
;; `(>= (file-mtime)
;;      (- (now) (date "24 hours")))
;;
(defmethod exec-interp :file-mtime [[_ file-expr] env]
  (let [location (:location env)
        location (if (nil? location) :local location)]
    (file-mtime ((file-for-type :location) file-expr env))))

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
      (let [result-set (j/query (:db-conn-info env) [sql-query] :as-arrays? true)
            result (first (second result-set))]
        (make-check-result
         {:result result
          :data result-set
          :messages sql-query})))))

(defn- process-group-args [args]
  (if (= (count args) 3)
    args
    (cons (gensym "group_") args)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod exec-interp :group [[_ & args] env]
  (let [[id desc checks] (process-group-args args)
        all-checks (:all-checks env)
        check-defs (map #(get all-checks %) checks)
        results (flatten (map #(exec-interp % env) check-defs))]
    (make-check-result
     {:result (every? identity (map result results))
      :data (map data results)
      :desc (str desc "( " (join ", " (map description results)) ")")
      :messages (map messages results)
      :exceptions (map exceptions results)
      :duration (apply + (remove nil? (map execution-duration results)))})))
