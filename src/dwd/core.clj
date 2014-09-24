(ns dwd.core
  "How can we check current state of things"
  {:author "Alex Bahouth"
   :date "Jan 20, 2014"}
  (:require [clojure.string :refer [join]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]])
  (:require [diesel.core :refer [definterpreter
                                 denamespace-form]]
            [roxxi.utils.print :refer [print-expr]]
            [roxxi.utils.common :refer [def-]]
            [clj-time.core :as t]
            [clj-time.coerce :as c])
  (:require [date-expr.core :as de])
  (:require [dwd.files.file :refer [file-present?
                                    file-mtime
                                    file-size
                                    file-hash
                                    file-stream]]
            [dwd.files.file-system :refer [list-files-matching-prefix
                                           make-file-listing]]
            [dwd.files.core :refer [file-for-type
                                    filesystem-for-type]]
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

;; date functions pass out to clj-time library
(def date-ops
  {'today t/today
   'yesterday #(t/minus (t/today) (t/days 1))
   'date-plus t/plus
   'date-minus t/minus
   'year t/year
   'month t/month
   'day t/day
   'hour t/hour
   'minute t/minute
   'second t/second
   'years t/years
   'months t/months
   'days t/days
   'hours t/hours
   'minutes t/minutes
   'seconds t/seconds})

(defn- date-op? [expr]
  (date-ops (first expr)))

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
  ['<= => :<=]
  ;; files
  ['file-present? => :file-present?]
  ['file-mtime => :file-mtime]
  ['file-hash => :file-hash]
  ['file-size => :file-size]
  ['file-stream => :file-stream]
  ['list-files-matching-prefix => :list-files-matching-prefix]
  ['list-files-in-date-range => :list-files-in-date-range]
  ['list-files-newer-than => :list-files-newer-than]
  ;; groups
  ['group => :group]
  ;; date info
  [date-op? => :date-op])

(defn exec-interp-namespaced [form env]
  (exec-interp (denamespace-form form) env))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## testing
;; ex. `'(testing DESC EXPR+)`
;;
(defmethod exec-interp :testing [[_ desc & exprs] env]
  (let [new-env (append-desc env desc)]
    (map #(exec-interp % new-env) exprs)))

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
        new-env (append-desc env desc)]
    (log/info "Executing check " id)
    (try
      (exec-interp expr new-env)
      (catch Exception e
        (log/spy e)))))

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
(defn- with-fs [config location env exprs]
  (let [config-keyword (keyword (str location "-config"))
        eval-config (exec-interp config env)
        new-env (merge env {:location location config-keyword eval-config})]
    (if (> (count exprs) 1)
      (make-check-result
       {:exception "Too many exprs in with-fs during exec"})
      (exec-interp (first exprs) new-env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-s3
;; ex. `'(with-s3 s3-config (file-present? /path/to/file)`
;;
;; on execution, there should only be a single expr used
(defmethod exec-interp :with-s3 [[_ s3-config & exprs] env]
  (with-fs s3-config "s3" env exprs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-ftp
;; ex. `'(with-ftp ftp-config (file-present? /path/to/file)`
;;
;; on execution, there should only be a single expr used
(defmethod exec-interp :with-ftp [[_ ftp-config & exprs] env]
  (with-fs ftp-config "ftp" env exprs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## with-sftp
;; ex. `'(with-sftp sftp-config (file-present? /path/to/file)`
;;
;; on execution, there should only be a single expr used
(defmethod exec-interp :with-sftp [[_ sftp-config & exprs] env]
  (with-fs sftp-config "sftp" env exprs))

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
                             results))]
    (make-check-result
     {:result result
      :data results
      :desc (:desc env)})))

(defmulti greater-equal? (fn [arg1 arg2] (class arg1)))
(defn- date-greater-equal? [arg1 arg2]
  (or (= arg1 arg2) (t/after? (c/to-date-time arg1) (c/to-date-time arg2))))
(defmethod greater-equal? java.util.Date [arg1 arg2]
  (date-greater-equal? arg1 arg2))
(defmethod greater-equal? org.joda.time.DateTime [arg1 arg2]
  (date-greater-equal? arg1 arg2))
(defmethod greater-equal? org.joda.time.LocalDate [arg1 arg2]
  (date-greater-equal? arg1 arg2))
(defmethod greater-equal? clojure.lang.Keyword [arg1 arg2]
  arg1)
(defmethod greater-equal? :default [arg1 arg2]
  (>= arg1 arg2))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## >=
;; ex. `' (>= >=expr1 >=expr2)`
(defmethod exec-interp :>= [[_ >=expr1 >=expr2] env]
  (let [results (map #(if (or (symbol? % ) (seq? %))
                                 (exec-interp % env)
                                 %)
                               [>=expr1 >=expr2])
        result (apply greater-equal? (doall (map #(if (satisfies? CheckResult %)
                                                    (.result %)
                                                    %)
                                                 results)))]
    (make-check-result
     {:result result
      :data results
      :desc (:desc env)})))

(defmulti lesser-equal? (fn [arg1 arg2] (class arg1)))
(defn- date-lesser-equal? [arg1 arg2]
  (or (= arg1 arg2) (t/before? (c/to-date-time arg1) (c/to-date-time arg2))))
(defmethod lesser-equal? java.util.Date [arg1 arg2]
  (date-lesser-equal? arg1 arg2))
(defmethod lesser-equal? org.joda.time.DateTime [arg1 arg2]
  (date-lesser-equal? arg1 arg2))
(defmethod lesser-equal? org.joda.time.LocalDate [arg1 arg2]
  (date-lesser-equal? arg1 arg2))
(defmethod lesser-equal? clojure.lang.Keyword [arg1 arg2]
  arg1)
(defmethod lesser-equal? :default [arg1 arg2]
  (<= arg1 arg2))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## >=
;; ex. `' (>= >=expr1 >=expr2)`
(defmethod exec-interp :<= [[_ <=expr1 <=expr2] env]
  (let [results (map #(if (or (symbol? % ) (seq? %))
                                 (exec-interp % env)
                                 %)
                               [<=expr1 <=expr2])
        result (apply lesser-equal? (map #(if (satisfies? CheckResult %)
                                            (.result %)
                                            %)
                                         results))]
    (make-check-result
     {:result result
      :data results
      :desc (:desc env)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-present?
;; `'(file-present? /path/to/file.txt)`
;;
(defmethod exec-interp :file-present? [[_ file-expr] env]
  (let [location (:location env)]
    (file-present? ((file-for-type location) file-expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-mtime
;; `(>= (file-mtime)
;;      (- (now) (date "24 hours")))
;;
(defmethod exec-interp :file-mtime [[_ file-expr] env]
  (let [location (:location env)]
    (file-mtime ((file-for-type location) file-expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-hash
;; `(= (file-hash a)
;;     (file-hash b))
;;
(defmethod exec-interp :file-hash [[_ file-expr] env]
  (let [location (:location env)]
    (file-hash ((file-for-type location) file-expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-size
;; `(>= (file-size)
;;      800000)
;;
(defmethod exec-interp :file-size [[_ file-expr] env]
  (let [location (:location env)]
    (file-size ((file-for-type location) file-expr env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## file-stream
;; `(file-stream)
;;
(defmethod exec-interp :file-stream [[_ file-expr] env]
  (let [location (:location env)]
    (file-stream ((file-for-type location) file-expr env))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## list-files-matching-prefix
;; `(list-files-matching-prefix "s3://okl-danger/yodaetl/resources/diag")`
;;
;; In S3 terms, this is equivalent to `s3cmd ls -r prefix`
;;
;; Takes 2 args:
;; - the prefix string to match against
;; - options map -- see dwd.files.file-system for details. Note that
;;   most/all of the options have not been implemented yet!
;;
;; Returns an ObjectListing with a lazy seq (!) in the :paths field. The
;; paths will be in order by date.
(defmethod exec-interp :list-files-matching-prefix [[_ prefix & [options]] env]
  (let [location (:location env)
        fs ((filesystem-for-type location) env)
        merged-options (merge options {:recursive true})]
    (list-files-matching-prefix fs prefix merged-options)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## list-files-in-date-range
;; `(list-files-in-date-range
;;      "s3://bucket/file/%Y/%m/%d"
;;      "s3://bucket/file/2014/08/01"
;;      "s3://bucket/file/2014/08/15")
;;
;; Takes 3 args:
;; - date-expr pattern-string
;; - date-expr pattern-string formatted with start time
;; - date-expr pattern-string formatted with end time
;;
;; Returns an ObjectListing with a lazy seq in the :paths field. The
;; paths will be in order by date.
;;
;; If the env has a truthy value for the key `:exclude-start?`,
;; then paths under the formatted-start will be excluded.
;; If the env has a truthy value for the key `:exclude-end?`,
;; then paths under the formatted-end will be excluded.
(defmethod exec-interp :list-files-in-date-range
  [[_ date-expr-string formatted-start formatted-end] env]
  (let [location (:location env)
        fs ((filesystem-for-type location) env)
        date-expr (de/make-date-expr date-expr-string)
        prefixes (de/formatted-date-range formatted-start
                                          formatted-end
                                          date-expr)
        prefixes (if (:exclude-start? env) (rest prefixes) prefixes)
        prefixes (if (:exclude-end? env) (butlast prefixes) prefixes)
        check-results (map #(list-files-matching-prefix fs % {:recursive true})
                           prefixes)]
    (when (empty? prefixes)
      (log/warnf "list-files-in-date-range generated an empty date-range!
                  Possibly formatted-start (%s) is in the future from
                  formatted-end (%s)?"
                 formatted-start
                 formatted-end))
    (if (empty? check-results)
      (first check-results)
      (xform-the-result-field (merge-check-results check-results)
                              #(make-file-listing (apply concat (map :paths %)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## list-files-newer-than
;; `(list-files-newer-than
;;      "s3://bucket/file/%Y/%m/%d"
;;      "America/Los_Angeles"
;;      "s3://bucket/file/2014/08/01")
;;
;; Args:
;; - date-expr pattern-string
;; - timezone for date-expr (if nil, utc will be used)(see danger-date-expr
;;     for acceptable timezone formats)
;; - formatted start-date
;;
;; First, a tz is determined:
;;   1) A TZ may be specified (in -0800 or in America/Los_Angeles format).
;;   2) If no TZ specified, check if a TZ is encoded in the date-expr. If so,
;;      extract the TZ from the formatted-start
;;   3) If no TZ specified, and no TZ in date-expr, assume UTC.
;; Then, a formatted-end expr is determined:
;;   the date-expr is formatted to "now" in that TZ.
;; Finally, list all files in the date-range between formatted-start and
;; formatted-end :)
;;
;; Returns an ObjectListing with a lazy seq in the :paths field. The
;; paths will be in order by date.
(def- default-tz nil)
(defmethod exec-interp :list-files-newer-than
  ;; Future plans:
  ;; - regex-filter option
  ;; - allow formatted start-date to be empty string.
  ;;     If the formatted-date-expr-string is empty string, the
  ;;     behavior is to take everything before the first
  ;;     date-conversion-specifier (i.e. before the first %Y or
  ;;     whatever), call that the prefix, and return
  ;;        (list-files-matching-prefix prefix)
  ;; - implement a smarter strategy (allowing optimization strategy hints?)
  ;;   for "infrequent" file based data feeds (like yoda) for which the naive
  ;;   list-every-instant-at-finest-granularity trick doesn't very well
  ;;     http://okl-danger.s3.amazonaws.com/yodaetl/jobs/2014-08-19-02-01-04/
  ;;     http://okl-danger.s3.amazonaws.com/yodaetl/jobs/2014-08-18-02-01-05/
  ;; - allow a "lookahead" option to look into the future by, say, 1 day,
  ;;   rather than just looking until now
  [[_ date-expr-string tz formatted-start] env]
  (let [has-tz (de/has-tz? date-expr-string)
        extracted-tz (when has-tz
                       (de/extract-timezone date-expr-string formatted-start))
        final-tz (cond
                  (not (nil? tz)) tz
                  has-tz   extracted-tz
                  :else    default-tz)
        date-expr (de/make-date-expr date-expr-string final-tz)
        now (:seconds (de/epoch-time-now))
        formatted-end (de/format-expr date-expr now)]
    (when (and has-tz
               (not= final-tz extracted-tz))
      (let [msg (format
                 "Warning! Date-expr and formatted-start have different
                  timezones! Date-expr has tz %s and formatted-start has
                  timezone %s"
                 final-tz
                 extracted-tz)]
        (log/error msg)
        (throw (RuntimeException. msg))))
    (exec-interp (list 'list-files-in-date-range
                       date-expr-string
                       formatted-start
                       formatted-end)
                 (merge env {:exclude-start? true}))))

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; time/date stuff
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod exec-interp :date-op [[op-name & args] env]
  (let [op (date-ops op-name)
        interped-args (map #(if (seq? %) (exec-interp % env) %) args)]
    (apply op interped-args)))
