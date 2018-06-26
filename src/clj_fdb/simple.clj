(ns clj-fdb.simple
  (:refer-clojure :rename {range core-range})
  (:require [clj-fdb.macros :refer [jfn]]
            [clj-fdb.interfaces :as ic]
            [clj-fdb.tuple]
            [clj-fdb.subspace])
  (:import (com.apple.foundationdb Database KeySelector MutationType Range
                                   TransactionContext)
           (clojure.lang Keyword)))

(def pack ic/pack)
(def range ic/range)

(defn range-starts-with
  [^"[B" prefix]
  (Range/startsWith prefix))


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

(defmulti clear
  "Clear one or more keys"
  (fn [& ys] (mapv class ys)))
(defmethod clear [TransactionContext (Class/forName "[B")] [txctx key]
  (.run txctx (jfn [tx]
                   (.clear tx key))))
(defmethod clear [TransactionContext (Class/forName "[B") (Class/forName "[B")]
  [txctx begin-key end-key]
  (.run txctx (jfn [tx]
                   (.clear tx begin-key end-key))))
(defmethod clear [TransactionContext Range] [txctx rng]
  (.run txctx (jfn [tx]
                   (.clear tx rng))))

(defmulti get-range
  "Do a range query"
  (fn [& ys] (mapv class ys)))
(defmethod get-range [TransactionContext Range] [db rng]
  (get-range db rng 20 :asc))
(defmethod get-range [TransactionContext Range Long] [db rng limit]
  (get-range db rng limit :asc))
(defmethod get-range [TransactionContext Range Long Keyword]
  [db rng limit direction]
  (.read db (jfn [tx]
                 (let [reverse? (case direction
                                  :asc false
                                  :desc true)]
                   (.join (.asList (.getRange tx rng limit reverse?)))))))
(defmethod get-range [TransactionContext KeySelector KeySelector] [db begin end]
  (.read db (jfn [tx]
                 (.join (.asList (.getRange tx begin end))))))
(defmethod get-range [TransactionContext KeySelector KeySelector Long]
  [db begin end limit]
  (.read db (jfn [tx]
                 (.join (.asList (.getRange tx begin end limit))))))
(defmethod get-range [TransactionContext KeySelector KeySelector Long Keyword]
  [db begin end limit direction]
  (.read db (jfn [tx]
                 (let [reverse? (case direction
                                  :asc false
                                  :desc true)]
                   (.join (.asList (.getRange tx begin end limit reverse?)))))))
