(ns clj-fdb.core-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]]
            [clj-fdb.core :refer :all]))

(deftest trivial
  (is (= 1 1)))
