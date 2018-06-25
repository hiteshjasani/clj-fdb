(ns app.core
  (:require [clj-fdb.db]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
            [clj-fdb.simple :as simp]))

(def subspace-name ["examples" "class-scheduling"])
(def ^:dynamic *db* nil)                ; db connection
(def ^:dynamic *ss* nil)                ; subspace

(defn open-db []
  (let [db       (clj-fdb.db/open)
        subspace (dir/create-or-open (dir/directory-layer) db subspace-name)]
    [db subspace]))

(defn test-db []
  (simp/put-val *db* (simp/pack *ss* "hello") (val/byte-arr "world"))
  (let [hello (val/to-str (simp/get-val *db* (simp/pack *ss* "hello")))]
    (println "Hello" hello)))

(defn -main
  [& args]
  (println "Starting class scheduler")
  (with-open [db (clj-fdb.db/open)]
    (let [ss (.join (dir/create-or-open (dir/directory-layer) db
                                        subspace-name))]
      (binding [*db* db *ss* ss]
        (test-db)
        (println "Ending class scheduler")))))
