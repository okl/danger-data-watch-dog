(ns dwd-service.handler
  "Handler class for service webserver"
  {:author "Eric Sayle"
   :date "Fri Jul 18 14:23:25 PDT 2014"}
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [clojure.java.io :as io]
            [ring.middleware.json :as middleware])
  (:require [dwd.core :refer :all]
            [dwd.id-interp :refer :all]))

(def base-check-file "tmp/check.out")
(defn- load-check-configs
  ([_] (load-check-configs))
  ([] (let [file-name base-check-file
            file (io/as-file file-name)]
        (when (.exists file)
          (with-open [reader (java.io.PushbackReader. (io/reader file))]
            (apply merge (map #(id-interp % {}) (read reader))))))))

(def all-checks (atom {}))
(defn- get-all-checks []
  (when (empty? @all-checks)
    (swap! all-checks load-check-configs))
  @all-checks)
(defn- add-check! [new-check]
  (let [old-checks @all-checks
        new-checks (id-interp new-check {})
        merged-checks (merge old-checks new-checks)]
    (swap! all-checks (fn [arg1 arg2] arg2) merged-checks)))

(defn- run-check! [check-id]
  (let [check-def (get (get-all-checks) check-id)
        results (exec-interp check-def {})]
    (if (seq? results)
      (clojure.string/join " " results)
      (str results))))

(defn- save-checks! []
  (io/make-parents base-check-file)
  (spit base-check-file (vals (get-all-checks))))


(defroutes app-routes
  (GET "/" [] "<h1>Hello World</h1>")
  (GET "/test" [] "Testing")
  (GET "/run/:check-id" [check-id]
    {:status 200
     :body (run-check! (symbol check-id))})
  (POST "/create-check" [data]
    (if (not data)
      {:status 400
       :body "Missing JSON field \"data\""}
      (let [parsed-data (read-string data)]
        {:status 200
         :body (add-check! parsed-data)})))
  (GET "/all-checks" []
    {:status 200
     :body (get-all-checks)})
  (POST "/save-checks" []
    {:status 200
     :body (save-checks!)})
  (route/not-found "<h1>Page not found</h1>"))


(def app
  (-> (handler/site app-routes)
      (middleware/wrap-json-response)
      (middleware/wrap-json-params)))
