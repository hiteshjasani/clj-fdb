(ns clj-fdb.simple
  (:refer-clojure :rename {range core-range})
  (:require [clj-fdb.macros :refer [jfn]]
            [clj-fdb.interfaces :as ic]
            [clj-fdb.tuple]
            [clj-fdb.subspace])
  (:import (com.apple.foundationdb Database KeySelector MutationType Range)
           (clojure.lang Keyword)))

(def pack ic/pack)

(def range ic/range)

(defn get-val
  [db k]
  (.read db (jfn [tx]
                 (.join (.get tx k)))))

(defn put-val
  [db k v]
  (.run db (jfn [tx]
                (.set tx k v))))

(defn put-vals
  [db m]
  (.run db (jfn [tx]
                (doseq [[k v] m]
                  (.set tx k v)))))

(defn atomic
  "
  https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/MutationType.html"
  [db k op param]
  (let [mt (case op
             :add                   MutationType/ADD
             :bit-and               MutationType/BIT_AND
             :bit-or                MutationType/BIT_OR
             :bit-xor               MutationType/BIT_XOR
             :byte-max              MutationType/BYTE_MAX
             :byte-min              MutationType/BYTE_MIN
             :max                   MutationType/MAX
             :min                   MutationType/MIN
             :version-stamped-key   MutationType/SET_VERSIONSTAMPED_KEY
             :version-stamped-value MutationType/SET_VERSIONSTAMPED_VALUE
             )]
    (.run db (jfn [tx]
                  (.mutate tx mt k param)))))

(defmulti get-range
  "Do a range query"
  (fn [& ys] (mapv class ys)))
(defmethod get-range [Database Range] [db rng] (get-range db rng 20 :asc))
(defmethod get-range [Database Range Long] [db rng limit]
  (get-range db rng limit :asc))
(defmethod get-range [Database Range Long Keyword] [db rng limit direction]
  (.read db (jfn [tx]
                 (let [reverse? (case direction
                                  :asc false
                                  :desc true)]
                   (.join (.asList (.getRange tx rng limit reverse?)))))))
(defmethod get-range [Database KeySelector KeySelector] [db begin end]
  (.read db (jfn [tx]
                 (.join (.asList (.getRange tx begin end))))))
(defmethod get-range [Database KeySelector KeySelector Long]
  [db begin end limit]
  (.read db (jfn [tx]
                 (.join (.asList (.getRange tx begin end limit))))))
(defmethod get-range [Database KeySelector KeySelector Long Keyword]
  [db begin end limit direction]
  (.read db (jfn [tx]
                 (let [reverse? (case direction
                                  :asc false
                                  :desc true)]
                   (.join (.asList (.getRange tx begin end limit reverse?)))))))
