(ns dwd.check-result
  {:author "Eric Sayle"
   :date "Thu Jul  3 11:39:56 PDT 2014"})


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
      (:desc config))
    (toString [_]
      (str config))))
