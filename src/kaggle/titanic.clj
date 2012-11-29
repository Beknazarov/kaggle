(ns kaggle.titanic
  (:use [kaggle.core]
        [clojure.tools.macro])
  (:require [clojure.string]))

(def train-csv "train.csv")
(def test-csv "test.csv")

(defn read-data [^String source]
  (slurp source))

(defn split-csv-line [line]
  (lazy-seq
   (let [matcher (re-matcher #"^((\".*\")|[^,]*)(,(.*))?$" line)
         find-result (re-find matcher)]
     (when find-result
       (let [groups (re-groups matcher)
             next-item (groups 1)
             remaining (groups 4)]
         (if remaining
           (cons next-item (split-csv-line-rec remaining))
           (list next-item)))))))

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
       [(csv-line-seq->data-items (rest xs) header)
        header])))

(defmacro with-reader-as-line-seq [[line-seq-name source] & body]
  `(with-open [rdr# (clojure.java.io/reader ~source)]
     (let [~line-seq-name (line-seq rdr#)]
       ~@body)))

(defmacro with-csv-reader [[csv-name source header] & body]
  (assert (or (symbol? header)
              (vector? header)
              (nil? header)))
  (let [capture-header (symbol? header)
        header-provided (vector? header)]
    `(with-reader-as-line-seq [line-seq# ~source]
       (let [~(if header-provided
                csv-name
                [csv-name (if capture-header header `header#)])
             (-> line-seq#
                 ~(if header-provided
                   `(csv-line-seq->data-items ~header)
                   `(csv-line-seq->data-items)))]
         ~@body))))

(defmacro with-csv-writer [[dest writer-fn header] & body]
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


;; (with-csv-reader [data "train.csv"]
;;   (aggregate [_ group-data] (group-by :sex data)
;;     (normalize
;;      (frequencies (map :survived group-data)))))

(defn gender-based-model []
  (with-csv-reader [data "test.csv" header]
    (with-csv-writer
      ["genderbasedmodel.csv"
       write-prediction (into [:survived] header)]
      (dorun
       (map
        (fn [x]
          (let [survived (if (= (x :sex) "female") 1 0)
                prediction (assoc x :survived survived)]
            (write-prediction prediction)))
        data)))))
