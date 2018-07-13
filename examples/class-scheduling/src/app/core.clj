(ns app.core
  (:require [clojure.pprint :as pp]
            [clj-fdb.db]
            [clj-fdb.macros :refer [jfn]]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
            [clj-fdb.simple :as simp]
            [mount.lite :as mount])
  (:import (com.apple.foundationdb Database TransactionContext)))

(def subspace-name ["examples" "class-scheduling"])

;; DB connection
(mount/defstate db
  :start (clj-fdb.db/open)
  :stop (.close @db))

;; Subspace for our example.
;;
;; The original example does not use a Subspace. But we're using one
;; since it's a good practice to use a namespace with your keys to
;; avoid collisions with other apps or examples using the same db.
(mount/defstate ss
  :start (.join (dir/create-or-open (dir/directory-layer) @db subspace-name))
  :stop (.join (dir/rm-if-exists!? (dir/directory-layer) @db subspace-name)))


(def class-levels ["intro" "for dummies" "remedial" "101" "201" "301" "mastery"
                   "lab" "seminar"])
(def class-types ["chem" "bio" "cs" "geometry" "calc" "alg" "film" "music"
                  "art" "dance"])
;; gen class times between 2:00 and 19:00 (inclusive)
(def class-times (mapv #(str % ":00") (range 2 20)))

(defn init-class-names []
  (for [lvl class-levels
        typ class-types
        tms class-times]
    (str tms " " typ " " lvl)))


(defn test-db []
  (simp/put-val @db (simp/pack @ss "hello") (val/byte-arr "world"))
  (let [hello (val/to-str (simp/get-val @db (simp/pack @ss "hello")))]
    (println "Hello" hello)))

(defn add-class
  [tx class-name]
  (simp/put-val tx (simp/pack @ss (tup/tuple "class" class-name))
                (val/byte-arr 100)))

(defn init
  [^Database db]
  (.run db (jfn [tx]
                ;; An easier way to clear the whole subspace than individually
                ;; clearing tuple ranges.
                ;; (simp/clear @db (simp/range @ss))

                (simp/clear tx (simp/range @ss "attends"))
                (simp/clear tx (simp/range @ss "class"))

                ;; Add classes
                (doseq [class-name (init-class-names)]
                  (add-class tx class-name))
                )))

(defn available-classes
  "List available classes"
  [^TransactionContext db]
  (.run db (jfn [tx]
                (->> (simp/get-range tx (simp/range @ss "class"))
                     ;; Return array of [class-name remaining-seat-count]
                     (map (fn [kv]
                            [(tup/to-str (.unpack @ss (.getKey kv)) 1)
                             (val/to-int (.getValue kv))]))
                     ;; Remove classes with no remaining seats
                     (filter #(> (second %) 0))
                     ;; Strip out seat count from final result
                     (mapv (fn [[class-name seat-count]]
                             class-name))))))

(defn signup
  [^TransactionContext db ^String s ^String c]
  (.run db (jfn [tx]
                (let [rec (simp/pack @ss (tup/tuple "attends" s c))]
                  (if (seq (simp/get-val tx rec))
                    nil                 ; student is already signed up
                    (let [seats-left (->> (simp/pack @ss "class" c)
                                          (simp/get-val tx)
                                          val/to-int)]
                      (when (zero? seats-left)
                        (throw (IllegalStateException. "No remaining seats")))

                      (simp/put-val tx (simp/pack @ss (tup/tuple "class" c))
                                    (val/byte-arr (dec seats-left)))
                      (simp/put-val tx rec (val/byte-arr ""))))))))

(defn drop-class
  [^TransactionContext db ^String s ^String c]
  (.run db (jfn [tx]
                (let [rec (simp/pack @ss (tup/tuple "attends" s c))]
                  (simp/clear tx rec)))))

(defn -main
  [& args]
  (println "Starting class scheduler")
  (mount/start)
  (test-db)

  (init @db)

  (println (format "%d classes are available" (count (available-classes @db))))

  (println "Ending class scheduler")
  (mount/stop))
