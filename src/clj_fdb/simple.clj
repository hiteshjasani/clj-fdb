(ns clj-fdb.simple
  (:refer-clojure :rename {range core-range})
  (:require [clj-fdb.macros :refer [jfn]]
            [clj-fdb.interfaces :as ic]
            [clj-fdb.tuple]
            [clj-fdb.subspace])
  (:import (com.apple.foundationdb Database KeySelector KeyValue MutationType
                                   Range TransactionContext)
           (clojure.lang Keyword)))

(def pack ic/pack)
(def range ic/range)

(defn range-starts-with
  [^"[B" prefix]
  (Range/startsWith prefix))


(defn get-val
  [^TransactionContext tx-ctx k]
  (.read tx-ctx (jfn [tx]
                     (.join (.get tx k)))))

(defn put-val
  [^TransactionContext tx-ctx k v]
  (.run tx-ctx (jfn [tx]
                    (.set tx k v))))

(defn put-vals
  [^TransactionContext tx-ctx m]
  (.run tx-ctx (jfn [tx]
                    (doseq [[k v] m]
                      (.set tx k v)))))

(defn atomic
  "
  https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/MutationType.html"
  [^TransactionContext tx-ctx k op param]
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
    (.run tx-ctx (jfn [tx]
                      (.mutate tx mt k param)))))

(defmulti clear
  "Clear one or more keys"
  (fn [& ys] (mapv class ys)))
(defmethod clear [TransactionContext (Class/forName "[B")] [tx-ctx key]
  (.run tx-ctx (jfn [tx]
                    (.clear tx key))))
(defmethod clear [TransactionContext (Class/forName "[B") (Class/forName "[B")]
  [tx-ctx begin-key end-key]
  (.run tx-ctx (jfn [tx]
                    (.clear tx begin-key end-key))))
(defmethod clear [TransactionContext Range] [tx-ctx rng]
  (.run tx-ctx (jfn [tx]
                    (.clear tx rng))))

(defmulti get-range
  "Do a range query"
  (fn [& ys] (mapv class ys)))
(defmethod get-range [TransactionContext Range] [tx-ctx rng]
  (.read tx-ctx (jfn [tx]
                     (.join (.asList (.getRange tx rng))))))
(defmethod get-range [TransactionContext Range Long] [tx-ctx rng limit]
  (get-range tx-ctx rng limit :asc))
(defmethod get-range [TransactionContext Range Long Keyword]
  [tx-ctx rng limit direction]
  (.read tx-ctx (jfn [tx]
                     (let [reverse? (case direction
                                      :asc false
                                      :desc true)]
                       (.join (.asList (.getRange tx rng limit reverse?)))))))
(defmethod get-range [TransactionContext KeySelector KeySelector]
  [tx-ctx begin end]
  (.read tx-ctx (jfn [tx]
                     (.join (.asList (.getRange tx begin end))))))
(defmethod get-range [TransactionContext KeySelector KeySelector Long]
  [tx-ctx begin end limit]
  (.read tx-ctx (jfn [tx]
                 (.join (.asList (.getRange tx begin end limit))))))
(defmethod get-range [TransactionContext KeySelector KeySelector Long Keyword]
  [tx-ctx begin end limit direction]
  (.read tx-ctx (jfn [tx]
                     (let [reverse? (case direction
                                      :asc false
                                      :desc true)]
                       (.join (.asList (.getRange tx begin end limit reverse?)))))))

(defn kv-get-key
  [^KeyValue kv]
  (.getKey kv))

(defn kv-get-value
  [^KeyValue kv]
  (.getValue kv))
