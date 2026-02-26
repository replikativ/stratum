(ns iforest-bench
  "Isolation Forest benchmark suite.

   Evaluates:
   1. Training and scoring speed (rows × trees)
   2. AUC-ROC on real ODDS datasets
   3. Comparison with PyOD (if available)
   4. Temporal evaluation: static vs rotating model

   Usage:
     clj -M:iforest              ;; all benchmarks
     clj -M:iforest speed        ;; speed only
     clj -M:iforest odds         ;; ODDS accuracy only
     clj -M:iforest pyod         ;; PyOD comparison
     clj -M:iforest temporal     ;; temporal rotation eval"
  (:require [stratum.iforest :as iforest]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.util Random]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- fmt-ms [^double ms] (format "%7.1fms" ms))

(defn- bench [f & {:keys [warmup iters] :or {warmup 10 iters 10}}]
  (dotimes [_ warmup] (f))
  (let [times (mapv (fn [_]
                      (let [start (System/nanoTime)
                            _ (f)
                            end (System/nanoTime)]
                        (/ (- end start) 1e6)))
                    (range iters))
        sorted (sort times)]
    {:median (nth sorted (quot iters 2))
     :min (first sorted)
     :p90 (nth sorted (int (* iters 0.9)))}))

(defn- auc-roc
  "AUC-ROC via trapezoidal rule."
  [^doubles scores ^longs labels ^long n]
  (let [pairs (sort-by first > (mapv (fn [i] [(aget scores i) (aget labels i)]) (range n)))
        n-pos (areduce labels i s (long 0) (+ s (aget labels i)))
        n-neg (- n n-pos)]
    (if (or (zero? n-pos) (zero? n-neg))
      Double/NaN
      (loop [tp 0.0, fp 0.0, prev-tp 0.0, prev-fp 0.0, auc 0.0, idx 0]
        (if (>= idx n)
          (let [tpr (/ tp n-pos) fpr (/ fp n-neg)
                prev-tpr (/ prev-tp n-pos) prev-fpr (/ prev-fp n-neg)]
            (+ auc (* 0.5 (- fpr prev-fpr) (+ tpr prev-tpr))))
          (let [[_ label] (nth pairs idx)
                new-tp (if (== label 1) (inc tp) tp)
                new-fp (if (== label 0) (inc fp) fp)]
            (if (or (= idx (dec n))
                    (not= (first (nth pairs idx))
                           (first (nth pairs (min (inc idx) (dec n))))))
              (let [tpr (/ new-tp n-pos) fpr (/ new-fp n-neg)
                    prev-tpr (/ prev-tp n-pos) prev-fpr (/ prev-fp n-neg)
                    area (* 0.5 (- fpr prev-fpr) (+ tpr prev-tpr))]
                (recur new-tp new-fp new-tp new-fp (+ auc area) (inc idx)))
              (recur new-tp new-fp prev-tp prev-fp auc (inc idx)))))))))

(defn- load-odds-csv
  "Load ODDS CSV (last col = label)."
  [^String path]
  (when (.exists (io/file path))
    (let [lines (vec (remove str/blank? (str/split-lines (slurp path))))
          has-header? (try (Double/parseDouble (first (str/split (first lines) #","))) false
                          (catch Exception _ true))
          data-lines (if has-header? (rest lines) lines)
          n (count data-lines)
          first-fields (str/split (first data-lines) #",")
          n-cols (count first-fields)
          d (dec n-cols)
          features (into-array (Class/forName "[D")
                     (mapv (fn [_] (double-array n)) (range d)))
          labels (long-array n)]
      (doseq [[i line] (map-indexed vector data-lines)]
        (let [fields (str/split line #",")]
          (dotimes [f d]
            (aset ^doubles (aget features f) i (Double/parseDouble (nth fields f))))
          (aset labels i (long (Double/parseDouble (nth fields d))))))
      {:features features :labels labels :n n :d d})))

(defn- features->map [features]
  (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features)))

;; ============================================================================
;; Speed Benchmarks
;; ============================================================================

(defn- bench-speed []
  (println "\n=== IForest Speed Benchmarks ===")
  (doseq [n [100000 1000000 10000000]]
    (let [d 5
          rng (Random. 42)
          features (into-array (Class/forName "[D")
                     (mapv (fn [_]
                             (let [arr (double-array n)]
                               (dotimes [i n] (aset arr i (.nextGaussian ^Random rng)))
                               arr))
                           (range d)))
          fmap (features->map features)]
      ;; Train speed
      (let [t0 (System/nanoTime)
            model (iforest/train {:from fmap :n-trees 100 :sample-size 256 :seed 42})
            train-ms (/ (- (System/nanoTime) t0) 1e6)]
        (println (format "\n  %,d rows × %d features:" n d))
        (println (format "    Train (100 trees × 256 sample): %s" (fmt-ms train-ms)))

        ;; Score speed
        (let [r (bench #(iforest/score model fmap) :warmup 5 :iters 10)]
          (println (format "    Score parallel:                 %s" (fmt-ms (:median r)))))

        ;; Score 1-thread
        (stratum.internal.ColumnOps/setParallelThreshold Integer/MAX_VALUE)
        (let [r (bench #(iforest/score model fmap) :warmup 5 :iters 10)]
          (println (format "    Score 1-thread:                 %s" (fmt-ms (:median r)))))
        (stratum.internal.ColumnOps/setParallelThreshold 200000)))))

;; ============================================================================
;; ODDS Accuracy Benchmarks
;; ============================================================================

(defn- bench-odds-dataset [name path expected-auc]
  (if-let [{:keys [features labels n d]} (load-odds-csv path)]
    (let [fmap (features->map features)
          t0 (System/nanoTime)
          model (iforest/train {:from fmap :n-trees 100 :sample-size 256 :seed 42})
          train-ms (/ (- (System/nanoTime) t0) 1e6)
          scores (iforest/score model fmap)
          auc (auc-roc scores labels n)
          n-anom (areduce labels i s (long 0) (+ s (aget labels i)))]
      (println (format "  %-15s n=%,7d d=%2d anom=%.1f%% AUC=%.4f (expect>%.2f) train=%s"
                       name n d (* 100.0 (/ (double n-anom) n)) auc expected-auc (fmt-ms train-ms)))
      {:name name :auc auc :n n :d d})
    (println (format "  %-15s SKIPPED (not found: %s)" name path))))

(defn- bench-odds []
  (println "\n=== ODDS Dataset Accuracy ===")
  (println "  (Run bin/download-odds to fetch datasets)")
  (bench-odds-dataset "Shuttle"      "data/odds/shuttle.csv"      0.95)
  (bench-odds-dataset "Http"         "data/odds/http.csv"         0.95)
  (bench-odds-dataset "ForestCover"  "data/odds/forestcover.csv"  0.80)
  (bench-odds-dataset "Mammography"  "data/odds/mammography.csv"  0.80)
  (bench-odds-dataset "Pima"         "data/odds/pima.csv"         0.55)
  (bench-odds-dataset "CreditCard"   "data/odds/creditcard.csv"   0.90))

;; ============================================================================
;; PyOD Comparison
;; ============================================================================

(defn- run-pyod-comparison [dataset-path dataset-name]
  "Run PyOD IsolationForest on the same dataset and compare AUC."
  (when (.exists (io/file dataset-path))
    (let [py-script (str "
import sys, time
import numpy as np
from sklearn.metrics import roc_auc_score
try:
    from pyod.models.iforest import IForest
except ImportError:
    print('PYOD_NOT_INSTALLED')
    sys.exit(0)
data = np.loadtxt('" dataset-path "', delimiter=',')
X, y = data[:, :-1], data[:, -1].astype(int)
t0 = time.time()
clf = IForest(n_estimators=100, max_samples=256, random_state=42)
clf.fit(X)
train_time = (time.time() - t0) * 1000
t0 = time.time()
scores = clf.decision_scores_
score_time = (time.time() - t0) * 1000
auc = roc_auc_score(y, scores)
print(f'{auc:.4f},{train_time:.1f},{score_time:.1f}')
")
          ^ProcessBuilder pb (ProcessBuilder.
                               ^"[Ljava.lang.String;"
                               (into-array String ["python3" "-c" py-script]))
          _ (.redirectErrorStream pb true)
          proc (.start pb)
          output (slurp (.getInputStream proc))
          _ (.waitFor proc)]
      (let [output (str/trim output)]
        (cond
          (str/includes? output "PYOD_NOT_INSTALLED")
          (println (format "    PyOD %-12s SKIPPED (pip install pyod)" dataset-name))

          (str/includes? output ",")
          (let [[auc train score] (str/split output #",")]
            (println (format "    PyOD %-12s AUC=%.4f train=%sms score=%sms"
                             dataset-name (Double/parseDouble auc) train score)))

          :else
          (println (format "    PyOD %-12s ERROR: %s" dataset-name output)))))))

(defn- bench-pyod []
  (println "\n=== PyOD Comparison ===")
  (println "  (pip install pyod scikit-learn to enable)")
  (run-pyod-comparison "data/odds/shuttle.csv"     "Shuttle")
  (run-pyod-comparison "data/odds/http.csv"        "Http")
  (run-pyod-comparison "data/odds/forestcover.csv" "ForestCover")
  (run-pyod-comparison "data/odds/mammography.csv" "Mammography")
  (run-pyod-comparison "data/odds/creditcard.csv"  "CreditCard"))

;; ============================================================================
;; Temporal Rotation Evaluation
;; ============================================================================

(defn- bench-temporal []
  (println "\n=== Temporal Rotation Evaluation ===")
  (println "  Synthetic data with concept drift (outlier region shifts at t=50K)")
  (let [n 100000
        d 3
        rng (Random. 42)
        ;; First half: outliers at [10,10,10]
        ;; Second half: outliers shift to [-10,-10,-10]
        features (into-array (Class/forName "[D")
                   (mapv (fn [_]
                           (let [arr (double-array n)]
                             (dotimes [i n]
                               (aset arr i (.nextGaussian ^Random rng)))
                             arr))
                         (range d)))
        labels (long-array n)
        ;; Inject outliers: 1% rate
        _ (dotimes [i 500]
            (let [idx (+ (* i 100) 50)  ;; every 100th row
                  sign (if (< idx 50000) 1.0 -1.0)]
              (aset labels idx 1)
              (dotimes [f d]
                (aset ^doubles (aget features f) idx (* sign 10.0)))))
        fmap (features->map features)
        ;; Split into 4 segments of 25K each
        segment-size 25000
        n-segments 4]

    ;; Static model: train on first 50%
    (let [train-fmap (into {} (map (fn [[k v]]
                                     [k (let [arr (double-array segment-size)]
                                          (System/arraycopy ^doubles v 0 arr 0 segment-size)
                                          arr)])
                                   fmap))
          static-model (iforest/train {:from train-fmap :n-trees 100 :sample-size 256 :seed 42})]
      (println "\n  Static model (trained on segment 1):")
      (doseq [seg (range n-segments)]
        (let [start (* seg segment-size)
              seg-fmap (into {} (map (fn [[k v]]
                                      [k (let [arr (double-array segment-size)]
                                           (System/arraycopy ^doubles v start arr 0 segment-size)
                                           arr)])
                                    fmap))
              seg-labels (let [arr (long-array segment-size)]
                           (System/arraycopy labels start arr 0 segment-size)
                           arr)
              scores (iforest/score static-model seg-fmap)
              auc (auc-roc scores seg-labels segment-size)]
          (println (format "    Segment %d [%,d-%,d]: AUC=%.4f" (inc seg) start (+ start segment-size) auc)))))

    ;; Rotating model: retrain 10 trees per segment
    (println "\n  Rotating model (10 trees rotated per segment):")
    (let [init-fmap (into {} (map (fn [[k v]]
                                    [k (let [arr (double-array segment-size)]
                                         (System/arraycopy ^doubles v 0 arr 0 segment-size)
                                         arr)])
                                  fmap))]
      (loop [model (iforest/train {:from init-fmap :n-trees 100 :sample-size 256 :seed 42})
             seg 0]
        (when (< seg n-segments)
          (let [start (* seg segment-size)
                seg-fmap (into {} (map (fn [[k v]]
                                        [k (let [arr (double-array segment-size)]
                                             (System/arraycopy ^doubles v start arr 0 segment-size)
                                             arr)])
                                      fmap))
                seg-labels (let [arr (long-array segment-size)]
                             (System/arraycopy labels start arr 0 segment-size)
                             arr)
                ;; Score BEFORE rotation (test-then-train)
                scores (iforest/score model seg-fmap)
                auc (auc-roc scores seg-labels segment-size)
                ;; Rotate after scoring
                updated (iforest/rotate-forest model seg-fmap 10)]
            (println (format "    Segment %d [%,d-%,d]: AUC=%.4f" (inc seg) start (+ start segment-size) auc))
            (recur updated (inc seg))))))))

;; ============================================================================
;; Main
;; ============================================================================

(defn -main [& args]
  (let [all-args (set (map str/lower-case (or args [])))]
    (println "")
    (println "================================================================")
    (println "  Isolation Forest Benchmark Suite")
    (println "================================================================")

    (when (or (empty? all-args) (contains? all-args "speed"))
      (bench-speed))
    (when (or (empty? all-args) (contains? all-args "odds"))
      (bench-odds))
    (when (or (empty? all-args) (contains? all-args "pyod"))
      (bench-pyod))
    (when (or (empty? all-args) (contains? all-args "temporal"))
      (bench-temporal))

    (println "\nBenchmark suite complete.")
    (shutdown-agents)))
