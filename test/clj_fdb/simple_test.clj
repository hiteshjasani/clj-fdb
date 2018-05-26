(ns clj-fdb.simple-test
  (:require [clojure.test :refer [run-tests deftest testing is are with-test
                                  use-fixtures]]
            [clj-fdb.db :as fdb]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.simple :refer :all]
            ))

(deftest making-directories
  (with-open [db (fdb/open)]
    (let [ss (.join (dir/create-or-open (dir/directory-layer) db
                                        ["test" "simple-test"]))]
      (try
        (put-val db (tup/pack ss "foo") (tup/pack "bar"))
        (let [v (tup/to-str (get-val db (tup/pack ss "foo")))]
          (println (str "type = " (type v)))
          (is (= 3 (count v)))
          (is (= java.lang.String (type v)))
          (is (= "bar" v)))
        (is (= "bar" (tup/to-str (get-val db (tup/pack ss "foo")))))
        (finally
          (.join (dir/rm-if-exists!? (dir/directory-layer) db ["test"]))))
      )))


(run-tests)
