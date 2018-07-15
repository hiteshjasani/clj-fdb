(ns clj-fdb.db
  (:import (com.apple.foundationdb Database FDB)))

(def FDBENV (FDB/selectAPIVersion 520))

(defn ^Database open
  ([]
   (.open FDBENV))
  ([env]
   (.open env)))

(defn close
  [^Database db]
  (.close db))
