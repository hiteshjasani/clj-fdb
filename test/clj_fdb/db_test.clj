(ns clj-fdb.db-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]]
            [clj-fdb.db :as fdb-db]))

(deftest open-db
  (with-open [db (fdb-db/open)]
    (is (.options db))))

(run-tests)
