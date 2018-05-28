(ns clj-fdb.simple
  (:require [clj-fdb.macros :refer [jfn]])
  (:import (com.apple.foundationdb MutationType Range)))

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

(defn get-range
  "Must have a range to do this query.

  (let [range (.range (tuple \"foo\" \"bar\"))]
    (get-range range 10 :asc))
  "
  ([db ^Range range] (get-range db range 20 :asc))
  ([db ^Range range ^long limit] (get-range db range 20 :asc))
  ([db ^Range range ^long limit direction]
   (.read db (jfn [tx]
                  (let [reverse? (case direction
                                   :asc false
                                   :desc true)]
                    (.join (.asList (.getRange tx range limit reverse?)))))))
  )
