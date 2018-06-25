(ns clj-fdb.directory-test
  (:require [clojure.test :refer [run-tests deftest testing is are with-test
                                  use-fixtures]]
            [clj-fdb.db :as fdb]
            [clj-fdb.directory :refer :all]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            ))

(deftest making-directories
  (with-open [db (fdb/open)]
    (let [ss (.join (create-or-open (directory-layer) db
                                    ["test" "directory-test"]))]
      (is (.join (exists? (directory-layer) db [])))
      (is (.join (exists? (directory-layer) db ["test"])))
      (is (.join (exists? (directory-layer) db ["test" "directory-test"])))
      ;; Following breaks for db's with multiple active subspaces
      #_(is (= ["test"] (.join (ls (directory-layer) db []))))
      (is (= ["directory-test"] (.join (ls (directory-layer) db ["test"]))))
      (is (= [] (.join (ls (directory-layer) db ["test" "directory-test"]))))
      (is (.join (rm-if-exists!? (directory-layer) db ["test"])))
      (is (.join (exists? (directory-layer) db [])))
      (is (false? (.join (exists? (directory-layer) db ["test"]))))
      (is (false? (.join (exists? (directory-layer) db ["test"
                                                        "directory-test"]))))
      ;; Following breaks for db's with multiple active subspaces
      #_(is (= [] (.join (ls (directory-layer) db []))))
      )))

(deftest making-multiple-subspaces
  (with-open [db (fdb/open)]
    (let [ss1 (.join (create-or-open (directory-layer) db
                                     ["test" "subspace1"]))
          ss2 (.join (create-or-open (directory-layer) db
                                     ["test" "subspace2"]))]
      (is (.join (exists? (directory-layer) db ["test" "subspace1"])))
      (is (.join (exists? (directory-layer) db ["test" "subspace2"])))
      (is (.join (rm-if-exists!? (directory-layer) db ["test"])))
      (is (.join (exists? (directory-layer) db [])))
      )))

(run-tests)
