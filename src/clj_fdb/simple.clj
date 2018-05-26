(ns clj-fdb.simple
  (:require [clj-fdb.macros :refer [jfn]]))

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
