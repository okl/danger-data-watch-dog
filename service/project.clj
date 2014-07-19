(defproject dwd-service "0.1.0-SNAPSHOT"
  :description "Web service layer for DWD"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [dwd "0.1.0-SNAPSHOT"]
                 [compojure "1.1.8"]
                 [ring/ring-json "0.3.1"]]
  :plugins [[lein-ring "0.8.11"]]
  :ring {:handler dwd-service.handler/app})
