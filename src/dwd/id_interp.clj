(ns dwd.id-interp
  "Process the check config to get one per end result, with ids"
  {:author "Eric Sayle"
   :date "Wed Jul 16 11:25:06 PDT 2014"}
  (:require [diesel.core :refer :all])
  )

;; I only need to interpert what could be grouping checks together
(definterpreter id-interp [env]
  ['testing => :testing]
  ['check => :check]
;  ['lookup-config => :lookup-config]
  ['define-config => :define-config]
  ;; Supporting Databases
  ['with-db => :with-db]
;  ['query => :query]
  ;; Supporting Files
  ['with-s3 => :with-s3]
  ['with-ftp => :with-ftp]
  ;; Predicates
;  ['= => :=]
;  ['>= => :>=]
;  ['file-present? => :file-present?]
  ;; Grouping
  ['group => :group])

(defn- apply-function-to-map-values [m f]
  (into {} (for [[k v] m] [k (f v)])))

(defn- add-list [inmap args]
  "Takes a map, and wraps each key in a list that starts with args and ends
   with the key value"
  (apply-function-to-map-values inmap #(apply list (concat args [%]))))

(defn- create-sublist [env exprs & args]
  (let [arg-configs (map #(id-interp % env) exprs)
        updated-configs (map #(add-list % args) arg-configs)]
    (apply merge updated-configs)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; testing
;; blah blah what it says everywhere else
(defmethod id-interp :testing [[_ desc & args ] env]
  (create-sublist env args 'testing desc))

(defn- process-check-args [args id-prefix]
  (let [gensym-prefix (str id-prefix "check_")]
        (if (= (count args) 3)
          args
          (cons (gensym gensym-prefix) args))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; check
;; blah blah
(defmethod id-interp :check [[_ & args] env]
  (let [id-prefix (:id-prefix env)
        [id desc expr] (process-check-args args id-prefix)]
    {id (list 'check id desc expr)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; define-config
(defmethod id-interp :define-config [[_ id config-map & exprs] env]
  (create-sublist env exprs 'define-config id config-map))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; with-db
(defmethod id-interp :with-db [[_ conn-info-or-sym & exprs] env]
  (create-sublist env exprs 'with-db conn-info-or-sym))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; with-s3
(defmethod id-interp :with-s3 [[_ s3-config & exprs] env]
  (create-sublist env exprs 'with-s3 s3-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; with-ftp
(defmethod id-interp :with-ftp [[_ ftp-config & exprs] env]
  (create-sublist env exprs 'with-ftp ftp-config))

(defn- process-group-args [args id-prefix]
  (let [gensym-prefix (str id-prefix "group_")]
    (if (= (count args) 3)
      args
      (cons (gensym "group_") args))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; group
(defmethod id-interp :group [[_ & args] env]
  (let [id-prefix (:id-prefix env)
        [id desc checks] (process-group-args args id-prefix)
        check-defs (filter seq? checks)
        group-def {id (list 'group id desc checks)}]
    (merge group-def (apply merge (map #(id-interp % {}) check-defs)))))
