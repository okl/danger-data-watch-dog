(ns dwd.config-interp
  ""
  {:author "Alex Bahouth & Eric Sayle"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log])
  (:require [diesel.core :refer :all]
            [roxxi.utils.print :refer :all]))


;; holds configuration symbols
(def config-sym-tab (atom {}))

(defn defconfig [k v]
  (when (contains? @config-sym-tab k)
    (log/warnf "Attempting to overwrite already defined function %s" k))
  (swap! config-sym-tab #(assoc % k v)))

(defn config-registry []
  @config-sym-tab)

(defn- append-desc [env-map desc]
  (update-in env-map [:desc] #(join % desc)))


(definterpreter config-interp [env]
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
(defmethod config-interp :check [[_ & args] env]
  (let [args (if (= (count args) 3)
               args
               (cons :anon args))
        id  (first args)
        desc (second args)
        expr (last args)]
    (str "check (with id " id ") " desc " " (config-interp expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## lookup-config
;; ex `'(with-s3 (lookup my-s3-config)` ... )
;;
;; This looks up a configuration defined using defconfig or define-config
(defmethod config-interp :lookup-config [[_ config-def] env]
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
(defmethod config-interp :define-config [[_ id config-map & exprs] env]
  (let [new-env (merge env {:config-registry (defconfig id config-map)})]
    (str "Using configuration with id " id " as " config-map "\n"
         (join "\n" (map #(config-interp % new-env) exprs)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; databases
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-db
;; ex.
;; ```
;; (with-db DB-CONN
;;   ...
;;   '(query QUERY)+)
;; ```
;;
(defmethod config-interp :with-db [[_ db-config & exprs] env]
  (let [db-config-real (config-interp db-config env)]
    (str "using database defined by " db-config-real "\n"
         (join "\n" (map #(config-interp % env) exprs)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-s3
;; ex. `'(with-s3 s3-config (file-present? /path/to/file)`
;;
(defmethod config-interp :with-s3 [[_ s3-config & exprs] env]
  (str "using s3 defined by " (config-interp s3-config env ) "\n"
       (join "\n" (map #(config-interp % env) exprs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-ftp
;; ex. `'(with-ftp ftp-config (file-present? /path/to/file)`
;;
(defmethod config-interp :with-ftp [[_ ftp-config & exprs] env]
  (str "using ftp defined by " (config-interp ftp-config env ) "\n"
       (join "\n" (map #(config-interp % env) exprs))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; predicates
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## =
;; ex. `'(= =expr1 =expr2 ...)`
(defmethod config-interp := [[_ & =exprs] env]
  (str "that " (join ", " =exprs) " are equal."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## >=
;; ex. `'(>= >=expr1 >=expr2 ...)`
(defmethod config-interp :>= [[_ >=expr1 >=expr2] env]
  (str "that " >=expr1 " is greater than " >=expr2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-present?
;; `'(file-present? /path/to/file.txt)`
;;
(defmethod config-interp :file-present? [[_ file-expr] env]
  (str "that file " file-expr " exists"))
