(ns clj-fdb.key-selector
  (:refer-clojure :rename {>= core->= > core->
                           <= core-<= < core-<})
  (:import (com.apple.foundationdb KeySelector)))

(defn >=
  [^"[B" key]
  (KeySelector/firstGreaterOrEqual key))

(defn >
  [^"[B" key]
  (KeySelector/firstGreaterThan key))

(defn <=
  [^"[B" key]
  (KeySelector/lastLessOrEqual key))

(defn <
  [^"[B" key]
  (KeySelector/lastLessThan key))
