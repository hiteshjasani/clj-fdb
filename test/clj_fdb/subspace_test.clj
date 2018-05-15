(ns clj-fdb.subspace-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]])
  (:use [clj-fdb.subspace :rename {range tuple-range}])
  (:import (com.apple.foundationdb.subspace Subspace)))

(deftest making-subspaces
  (is (= (Subspace.) (subspace)))
  (is (= "Subspace(rawPrefix=\\x02foo\\x00)" (as-str (subspace "foo"))))
  )

(run-tests)
