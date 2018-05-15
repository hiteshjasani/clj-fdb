(ns clj-fdb.core-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]])
  (:use [clj-fdb.tuple :rename {range tuple-range}])
  (:import (com.apple.foundationdb.tuple Tuple)))

(deftest making-tuples
  (is (= (Tuple.) (tuple nil)))
  (is (= "foo" (as-str (tuple "foo"))))
  )

(run-tests)
