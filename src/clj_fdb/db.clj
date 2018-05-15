(ns clj-fdb.db
  (:import (com.apple.foundationdb Database FDB)))

(def FDBENV (FDB/selectAPIVersion 510))

(defn ^Database open
  []
  (.open FDBENV))
