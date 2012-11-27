(ns kaggle.titanic
  (:use [kaggle.core]
        [clojure.tools.macro])
  (:require [clojure.string]))

(def train-csv "train.csv")
(def test-csv "test.csv")

(defn read-data [^String source]
  (slurp source))

(defn split-csv-line [line]
  (map (fn [x]
         (if (or (= x ",") (= x ",,")) nil x))
       (re-seq #"\".*\"|[^,]+|,,|,$|^," line)))

(defn csv-line-seq->data-items
  ([xs header]
     ;; If things fail, thrown an appropriate error message.
     ;; Need to handle quoting/escaping properly.
     (map (fn [x]
            (let [pieces (split-csv-line x)]
              (assert (= (count pieces) (count header)))
              (zipmap header pieces)))
          xs))
  ([xs]
     (let [header (map keyword (split-csv-line (first xs)))]
       (csv-line-seq->data-items (rest xs) header))))

(defmacro with-reader-as-line-seq [[line-seq-name source] & body]
  `(with-open [rdr# (clojure.java.io/reader ~source)]
     (let [~line-seq-name (line-seq rdr#)]
       ~@body)))

(defmacro with-csv-reader [[csv-name source header] & body]
  `(with-reader-as-line-seq [line-seq# ~source]
     (let [~csv-name
           (-> line-seq#
               ~(if header
                  `(csv-line-seq->data-items ~header)
                  `(csv-line-seq->data-items)))]
       ~@body)))

(defmacro with-csv-writer [[writer-fn dest header] & body]
  (assert (not (nil? header)))
  `(with-open [wrtr# (clojure.java.io/writer ~dest)]
     (.write wrtr# (clojure.string/join "," (map name ~header)))
     (.newLine wrtr#)
     (let [~writer-fn
           (fn [x#]
             (->> ~header
                  (map #(x# %1))
                  (clojure.string/join ",")
                  (.write wrtr#))
             (.newLine wrtr#))]
       ~@body)))

(defn header->idx-lookup-table [header]
  (zipmap header (range)))

;; Aggregation
(defn normalize [dist]
  (let [z (apply + (vals dist))]
    (->> dist
         (map (fn [[k v]] [k (/ v z)]))
         (into {}))))

(defmacro aggregate [[group-name group] grouped-data & body]
  `(->> ~grouped-data
        (map (fn [[~group-name ~group]]
               (let [value# (do ~@body)]
                 [~group-name value#])))
        (into {})))
