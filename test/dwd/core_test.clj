(ns dwd.core-test
 (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.string :refer [join]])
  (:require [dwd.core :refer :all]))

(def file-present-test
  '(file-present? "project.clj"))

