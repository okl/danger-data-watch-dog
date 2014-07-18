(ns dwd.icinga-interp
  "Generate icinga configuration for our checks"
  {:author "Eric Sayle"
   :date "Thu Jul 10 11:29:26 PDT 2014"}
  (:require [clojure.java.io :as io]
            [clojure.string :refer [join]])
  (:require [clj-yaml.core :as yaml]
            [clostache.parser :refer [render-resource]])
  (:require [diesel.core :refer :all]))

(def config
  (yaml/parse-string (slurp (io/resource "icinga.yml"))))

(defn- delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its contents.
Raise an exception if any deletion fails unless silently is true. Blatantly
stolen from clojure-contrib code
http://clojure.github.io/clojure-contrib/io-api.html"
  [f & [silently]]
  (let [f (io/file f)]
    (if (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-file-recursively child silently)))
    (io/delete-file f silently)))

(defn- joins [& args]
  (join " " args))

(defn- get-hostname []
  (get config :hostname))

(defn- get-url []
  (:url config))

(defn- append-desc [env-map desc]
  (update-in env-map [:desc] #(joins % desc)))


(definterpreter icinga-interp [env]
  ['testing => :testing]
  ['check => :check]
;; icinga config only needs to go as deep as check
;  ['lookup-config => :lookup-config]
  ;; Supporting Databases
;  ['with-db => :with-db]
;  ['query => :query]
  ;; Supporting Files
;  ['with-s3 => :with-s3]
;  ['with-ftp => :with-ftp]
  ;; Predicates
;  ['= => :=]
;  ['>= => :>=]
;  ['file-present? => :file-present?]
  ['group => :group])

(defprotocol IcingaConfig
  (config-id [_])
  (config-string [_])
  (config-dir [_]))

(defn- generate-service-config [id desc]
  (reify
    IcingaConfig
    (config-string [_]
      (render-resource "icinga/service.cfg"
                       {:description id
                        :hostname (get-hostname)
                        :displayname desc}))
    (config-dir [_]
      "services")
    (config-id [_]
      id)))


(defn- generate-servicegroup-config [id desc checks]
  (let [hostname (get-hostname)
        members (join "," (map #(join "," [hostname %]) checks))]
    (reify
      IcingaConfig
      (config-string [_]
        (render-resource "icinga/servicegroup.cfg"
                         {:name id
                          :description desc
                          :members members}))
      (config-dir [_]
        "servicegroups")
      (config-id [_]
        id))))

(defn- generate-host-config []
  (reify
    IcingaConfig
    (config-dir [_]
      "hosts")
    (config-id [_]
      "dwd")
    (config-string [_]
      (render-resource "icinga/host.cfg"
                       {:hostname (get-hostname)}))))

(defn- generate-command-config []
  (reify
    IcingaConfig
    (config-dir [_]
      "commands")
    (config-id [_]
      "run_check")
    (config-string [_]
      (render-resource "icinga/command.cfg"))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## testing
;; ex. `'(testing DESC EXPR+)`
;;
(defmethod icinga-interp :testing [[_ desc & exprs] env]
  (let [new-env (append-desc env desc)]
    (map #(icinga-interp % new-env) exprs)))


 (defn- process-check-args [args]
  (if (= (count args) 3)
    args
    (cons (gensym "check_") args)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## check
;; ex. `'(check DESC EXPR)`
;;
;; this is the configuration of a check
(defmethod icinga-interp :check [[_ & args] env]
  (let [[id desc expr] (process-check-args args)
        new-env (append-desc env desc)]
    (generate-service-config
     id
     (:desc new-env))))

(defn- process-group-args [args]
  (if (= (count args) 3)
    args
    (cons (gensym "group_") args)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## group
;; ex. `(group my-group "This is all of the ET checks"
;;             [et-check-1
;;             et-check-2
;;             ...])
(defmethod icinga-interp :group [[_ & args] env]
  (let [[id desc checks] (process-group-args args)
        check-defs (filter seq? checks)
        check-ids (filter #(not (seq? %)) checks)
        check-config (map #(icinga-interp % env) check-defs)
        group-config (generate-servicegroup-config
                      id
                      desc
                      (concat check-ids (map config-id check-config)))]
    (cons group-config check-config)))

;; write the config file for each piece of configuration

(defn- write-config [icinga-config output-dir]
  (let [outputfile (join "/" [output-dir
                              (config-dir icinga-config)
                              (str (config-id icinga-config) ".cfg")])]
    (io/make-parents outputfile)
    (spit outputfile (config-string icinga-config))))

(defn process-file [file & {:keys [output-dir] :or {output-dir "output"}}]
  (when (.exists (io/as-file output-dir))
    (delete-file-recursively output-dir))
  (with-open [reader (java.io.PushbackReader. (io/reader file))]
    (let [exprs (read reader)
          icinga-configs (flatten (map #(icinga-interp % {}) exprs))
          icinga-configs (conj icinga-configs (generate-host-config))
          icinga-configs (conj icinga-configs (generate-command-config))]
      (doall (map #(write-config % output-dir) icinga-configs)))))
