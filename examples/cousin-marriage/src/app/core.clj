(ns app.core
  (:require [clojure.pprint :as pp]
            [clojure.data.csv :as csv]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clj-fdb.macros :refer [jfn]]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
            [clj-fdb.simple :as simp]
            [mount.lite :as mount]
            [app.db :as adb])
  (:import (com.apple.foundationdb Database Range TransactionContext)
           (com.apple.foundationdb.subspace Subspace)))

(def dataset-filename "data/cousin-marriage-data.csv")

(defn read-csv
  [filename]
  (with-open [reader (io/reader filename)]
    (->> (doall (csv/read-csv reader))
         (drop 1)                       ; exclude row with labels
         (map (fn [[country s-perc]]
                [country (edn/read-string s-perc)]))
         (into {}))))

(def dataset (read-csv dataset-filename))

(defn add-country-percentage
  [^Database db ^Subspace ss ^String country ^Double percentage]
  (.run db (jfn [tx]
                (let [k1 (simp/pack ss (tup/tuple "country" country))
                      k2 (simp/pack ss (tup/tuple "revperc" (- 100.0 percentage) country))]
                  (simp/put-val tx k1 (val/byte-arr percentage))
                  (simp/put-val tx k2 (val/byte-arr percentage))
                  ))))

(defn add-countries
  [^Database db ^Subspace ss country-map]
  (doseq [[country percentage] country-map]
    (println (format "Adding %s - %f", country percentage))
    (add-country-percentage db ss country percentage)))

(defn print-country-perc
  [^long skip-keys ^Subspace ss kv-entries]
  (doseq [kv kv-entries]
    (println
     (format "%20s\t%2.1f"
             (str (first (drop skip-keys (tup/to-strs (ss/unpack ss (simp/kv-get-key kv))))))
             (val/to-double (simp/kv-get-value kv))))))

(defn print-subspace
  [^Database db ^Subspace ss]
  (let [rng (simp/range ss)
        entries (simp/get-range db rng)]
    (doseq [kv entries]
      (println (format "key: %s,  value: %f"
                       (str (vec (tup/to-strs (ss/unpack ss (simp/kv-get-key kv)))))
                       (val/to-double (simp/kv-get-value kv)))))))

(defn print-hr [] (println (apply str (repeat 75 \-))))

(defn -main
  [& args]
  (mount/start)

  ;; Start from pristine state
  (simp/clear @adb/db @adb/ss)

  ;; pretty print the raw dataset
  #_(pp/pprint dataset)

  ;; add the dataset to fdb
  (add-countries @adb/db @adb/ss dataset)

  ;; debug print the entire subspace
  #_(print-subspace @adb/db @adb/ss)

  (print-hr)
  (println "Top 5 Countries By Percentage" \newline)
  (print-country-perc 2
   @adb/ss
   (simp/get-range @adb/db (simp/range @adb/ss (tup/tuple "revperc")) 5 :asc))

  (print-hr)
  (println "Bottom 5 Countries By Percentage" \newline)
  (print-country-perc 2
   @adb/ss
   (simp/get-range @adb/db (simp/range @adb/ss (tup/tuple "revperc")) 5 :desc))

  (print-hr)
  (println "Top 5 Countries Alphabetically" \newline)
  (print-country-perc 1
   @adb/ss
   (simp/get-range @adb/db (simp/range @adb/ss (tup/tuple "country")) 5 :asc))

  (print-hr)
  (println "Bottom 5 Countries Alphabetically" \newline)
  (print-country-perc 1
   @adb/ss
   (simp/get-range @adb/db (simp/range @adb/ss (tup/tuple "country")) 5 :desc))

  ;; Clean up our changes
  (simp/clear @adb/db @adb/ss)

  (mount/stop))
