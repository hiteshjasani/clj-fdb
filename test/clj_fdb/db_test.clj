(ns clj-fdb.db-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]]
            [clj-fdb.db :as fdb-db])
  (:import (com.apple.foundationdb FDB)))

(deftest open-db
  (testing "with no parameters"
    (with-open [db (fdb-db/open)]
      (is (.options db))))
  #_(testing "with api version 5.1.x"
    (with-open [db (fdb-db/open (FDB/selectAPIVersion 510))]
      (is (.options db))))
  (testing "with api version 5.2.x"
    (with-open [db (fdb-db/open (FDB/selectAPIVersion 520))]
      (is (.options db))))
  )

(run-tests)
