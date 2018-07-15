(ns app.core
  (:require [clojure.pprint :as pp]
            [clj-fdb.db]
            [clj-fdb.macros :refer [jfn run-tx read-tx]]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
            [clj-fdb.simple :as simp]
            [mount.lite :as mount])
  (:import (com.apple.foundationdb Database TransactionContext)
           (com.apple.foundationdb.subspace Subspace)))

(def subspace-name ["examples" "class-scheduling"])

;; DB connection
(mount/defstate db
  :start (clj-fdb.db/open)
  :stop (clj-fdb.db/close @db))

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

(def class-names (init-class-names))

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
  (run-tx db
          (jfn [tx]
               ;; An easier way to clear the whole subspace than individually
               ;; clearing tuple ranges.
               (simp/clear tx @ss)

               ;; Add classes
               (doseq [class-name (init-class-names)]
                 (add-class tx class-name))
               )))

(defn available-classes
  "List available classes"
  [^TransactionContext db]
  (run-tx db
          (jfn [tx]
               (->> (simp/get-range tx (simp/range @ss "class"))
                    ;; Return array of [class-name remaining-seat-count]
                    (map (fn [kv]
                           [(tup/to-str (ss/unpack @ss (simp/kv-get-key kv)) 1)
                            (val/to-int (simp/kv-get-value kv))]))
                    ;; Remove classes with no remaining seats
                    (filter #(> (second %) 0))
                    ;; Strip out seat count from final result
                    (mapv (fn [[class-name seat-count]]
                            class-name))))))

(defn signup
  [^TransactionContext db ^String s ^String c]
  (run-tx db
          (jfn [tx]
               ;; Check if the student is already signed up.  If we have
               ;; a record, we'll just return nil.  Otherwise, we'll
               ;; check if seats are left and sign them up.
               (let [rec (simp/pack @ss (tup/tuple "attends" s c))]
                 (when (nil? (simp/get-val tx rec))
                   (let [class-key  (simp/pack @ss (tup/tuple "class" c))
                         seats-left (-> (simp/get-val tx class-key)
                                        val/to-int)]
                     (when (zero? seats-left)
                       (throw (IllegalStateException. "No remaining seats")))

                     (when (= 5 (count (simp/get-range tx (simp/range @ss (tup/tuple "attends" s)))))
                       (throw (IllegalStateException. "Too many classes")))

                     (simp/put-val tx class-key
                                   (val/byte-arr (dec seats-left)))
                     (simp/put-val tx rec (val/byte-arr ""))))))))

(defn drop-class
  [^TransactionContext db ^String s ^String c]
  (run-tx db
          (jfn [tx]
               ;; Check if the student is enrolled in the class.  If they
               ;; aren't then we don't have to do anything.  Otherwise
               ;; we'll drop them from the class.
               (let [rec (simp/pack @ss (tup/tuple "attends" s c))]
                 (when (seq (simp/get-val tx rec))
                   (let [class-key  (simp/pack @ss "class" c)
                         seats-left (-> (simp/get-val tx class-key)
                                        val/to-int)]
                     (simp/put-val class-key (val/byte-arr (inc seats-left)))
                     (simp/clear tx rec)))))))

(defn switch-classes
  [^TransactionContext db
   ^String student
   ^String old-class
   ^String new-class]
  (run-tx db (jfn [tx]
                  (drop-class db student old-class)
                  (signup db student new-class))))

(defn simulate-students
  "This algorithm is similar to the Java one with some differences.
     o
  "
  [^Database db i ops]
  (let [student-id  (str "s" i)
        my-classes  (atom {})
        all-classes class-names
        next-mood   (fn [class-count]
                      (cond
                        (and (> class-count 0)
                             (< class-count 5)) (rand-nth ["drop" "switch"
                                                           "add"])
                        (> class-count 0)       (rand-nth ["drop" "switch"])
                        (< class-count 5)       "add"))]
    (dotimes [j ops]
      (try
        (let [mood (next-mood (count @my-classes))]
          (condp = mood
            "add"    (let [c (rand-nth all-classes)]
                       (signup db student-id c)
                       (swap! my-classes assoc c true))
            "drop"   (let [c (rand-nth (keys @my-classes))]
                       (drop-class db student-id c)
                       (swap! my-classes dissoc c))
            "switch" (let [old-class (rand-nth (keys @my-classes))
                           new-class (rand-nth all-classes)]
                       (switch-classes db student-id old-class new-class)
                       (swap! my-classes (fn [m]
                                           (-> m
                                               (dissoc old-class)
                                               (assoc new-class true)))))
            ))
        (catch Exception e
          (println (str e ", need to recheck classes")))))))

(defn run-sim
  [^Database db num-students num-ops-per-student]
  (let [sims (mapv (fn [idx]
                     (future
                       (simulate-students db idx num-ops-per-student)))
                   (range num-students))]
    (doseq [sim sims]
      @sim)
    (println (format "Ran %d transactions" (* num-students
                                              num-ops-per-student)))))

(defn print-attendance
  [^Database db]
  (let [rng (simp/range @ss (tup/tuple "attends"))
        roster (simp/get-range db rng)]
    (doseq [kv roster]
      (println (format "student/class: %s"
                       (str (vec (tup/to-strs (ss/unpack @ss (simp/kv-get-key kv))))))))))

(defn print-subspace
  [^Database db ^Subspace ss]
  (let [rng (simp/range ss)
        entries (simp/get-range db rng)]
    (doseq [kv entries]
      (println (str (vec (tup/to-strs (ss/unpack ss (simp/kv-get-key kv)))))))))

(defn -main
  [& args]
  (println "Starting class scheduler")
  (mount/start)
  (test-db)

  ;; clear the subspace
  (simp/clear @db @ss)

  (init @db)
  (println (format "%d classes are available"
                   (count (available-classes @db))))

  (run-sim @db 5 5)
  (print-attendance @db)

  ;; Print contents of entire subspace
  #_(print-subspace @db @ss)

  ;; clear the subspace
  (simp/clear @db @ss)
  (shutdown-agents)
  (println "Ending class scheduler")
  (mount/stop))
