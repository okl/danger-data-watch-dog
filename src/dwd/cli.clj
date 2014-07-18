(ns dwd.cli
  "Command line driver for DWD"
  {:author "Eric Sayle"
   :date "Wed Jul 16 16:48:09 PDT 2014"}
  (:require [clojure.java.io :as io])
  (:require [dwd.core :refer [exec-interp]]
            [dwd.id-interp :refer [id-interp]]
            [dwd.icinga-interp :refer [process-file]]
            [dwd.check-result :refer :all]))

(def base-check-file "tmp/check.out")

(defn- load-check-configs [file-name & {:keys [quiet] :or {quiet false}}]
  (let [file (io/as-file file-name)]
    (if (not (.exists file))
      (when (not quiet)
        (throw (java.lang.IllegalArgumentException.
                (str "File " file-name " is unable to be loaded"))))
      (with-open [reader (java.io.PushbackReader. (io/reader file))]
        (apply merge (map #(id-interp % {}) (read reader)))))))

(defn- generate-icinga-config
  ([_] (process-file base-check-file))
  ([_ output-dir] (process-file base-check-file :output-dir output-dir)))

(defn- run-single-check [all-checks check-id]
  (let [check ((symbol check-id) all-checks)]
    (if (nil? check)
      (throw (java.lang.IllegalStateException.
              (str "Check " check-id " not found")))
      (exec-interp check {:all-checks all-checks}))))

(defn- exec-check [_ & checks-to-execute]
  (let [checks (load-check-configs base-check-file)
        results (map #(run-single-check checks %) checks-to-execute)
        final-result (every? true? (map result results)) ]
    (doseq [result results]
      (println "result is " result))
    (println "Final result is " final-result)))

(defn- import-file [_ file]
  (let [new-checks (load-check-configs file)
        old-checks (load-check-configs base-check-file)
        merged-checks (merge old-checks new-checks)]
    (println new-checks)
    (println old-checks)
    (io/make-parents base-check-file)
    (spit base-check-file (vals merged-checks))))

(defn- usage []
  (println "Usage: lein run COMMAND")
  (println)
  (println "Commands:")
  (println "icinga [output-dir]")
  (println "\tGenerate icinga config")
  (println "\tdefault output-dir is output")
  (println "exec-check id ...")
  (println "\texecute checks specified on command line")
  (println "\treturn code is 0 if all results are true")
  (println "import-file file-name")
  (println "\tload new config file into own repository"))

(defn -main [& args]
  (case (first args)
    "icinga" (apply generate-icinga-config args)
    "exec-check" (apply exec-check args)
    "import-file" (apply import-file args)
    (usage)))
