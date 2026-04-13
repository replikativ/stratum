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
  "AUC-ROC via trapezoidal rule. Primitive arrays only — no boxing."
  [^doubles scores ^longs labels ^long n]
  (let [n-pos (areduce labels i s (long 0) (+ s (aget labels i)))
        n-neg (- n n-pos)]
    (if (or (zero? n-pos) (zero? n-neg))
      Double/NaN
      ;; Build sorted-scores and sorted-labels arrays (descending by score)
      (let [^"[Ljava.lang.Integer;" idx-box
            (let [^"[Ljava.lang.Integer;" a (make-array Integer n)]
              (dotimes [i n] (aset a i (Integer/valueOf i)))
              (java.util.Arrays/sort a
                (reify java.util.Comparator
                  (compare [_ a b]
                    (Double/compare (aget scores (int b)) (aget scores (int a))))))
              a)
            sorted-scores (double-array n)
            sorted-labels (long-array n)
            _ (dotimes [i n]
                (let [ix (int (aget idx-box i))]
                  (aset sorted-scores i (aget scores ix))
                  (aset sorted-labels i (aget labels ix))))
            inv-n-pos (/ 1.0 (double n-pos))
            inv-n-neg (/ 1.0 (double n-neg))]
        (loop [tp 0.0, fp 0.0, prev-tp 0.0, prev-fp 0.0, auc 0.0, i (int 0)]
          (if (>= i n)
            (let [tpr (* tp inv-n-pos) fpr (* fp inv-n-neg)
                  prev-tpr (* prev-tp inv-n-pos) prev-fpr (* prev-fp inv-n-neg)]
              (+ auc (* 0.5 (- fpr prev-fpr) (+ tpr prev-tpr))))
            (let [label (aget sorted-labels i)
                  new-tp (if (== label 1) (+ tp 1.0) tp)
                  new-fp (if (== label 0) (+ fp 1.0) fp)
                  next-i (unchecked-inc-int i)]
              (if (or (>= next-i n)
                      (not= (aget sorted-scores i) (aget sorted-scores next-i)))
                (let [tpr (* new-tp inv-n-pos) fpr (* new-fp inv-n-neg)
                      prev-tpr (* prev-tp inv-n-pos) prev-fpr (* prev-fp inv-n-neg)
                      area (* 0.5 (- fpr prev-fpr) (+ tpr prev-tpr))]
                  (recur new-tp new-fp new-tp new-fp (+ auc area) next-i))
                (recur new-tp new-fp prev-tp prev-fp auc next-i)))))))))

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
          ^"[[D" features (into-array (Class/forName "[D")
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
          ^longs labels labels
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

(defn- ensure-odds-data
  "Download ODDS datasets if not present. Requires python3 with scipy in PATH."
  []
  (when-not (.exists (io/file "data/odds/shuttle.csv"))
    (println "  ODDS data not found — downloading via bin/download-odds ...")
    (let [script (io/file "bin/download-odds")]
      (if (.exists script)
        (let [^ProcessBuilder pb (ProcessBuilder. ^"[Ljava.lang.String;"
                                   (into-array String ["bash" "bin/download-odds"]))
              _ (.inheritIO pb)
              proc (.start pb)
              exit (.waitFor proc)]
          (when-not (zero? exit)
            (println "  WARNING: download-odds failed (exit" exit "). Install scipy: pip install scipy")))
        (println "  WARNING: bin/download-odds not found")))))

(defn- bench-odds []
  (println "\n=== ODDS Dataset Accuracy ===")
  (ensure-odds-data)
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
  "Run Stratum and PyOD IsolationForest on the same dataset, print side by side."
  (if-not (.exists (io/file dataset-path))
    (println (format "  %-15s SKIPPED (no data: %s — run bin/download-odds)" dataset-name dataset-path))
    (let [{:keys [features labels n d]} (load-odds-csv dataset-path)
          fmap (features->map features)
          ^longs labels labels
          ;; Stratum
          t0 (System/nanoTime)
          model (iforest/train {:from fmap :n-trees 100 :sample-size 256 :seed 42})
          stratum-train-ms (/ (- (System/nanoTime) t0) 1e6)
          t1 (System/nanoTime)
          stratum-scores (iforest/score model fmap)
          stratum-score-ms (/ (- (System/nanoTime) t1) 1e6)
          stratum-auc (auc-roc stratum-scores labels n)
          ;; PyOD
          py-script (str "
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
scores = clf.decision_function(X)
score_time = (time.time() - t0) * 1000
auc = roc_auc_score(y, scores)
print(f'{auc:.4f},{train_time:.1f},{score_time:.1f}')
")
          ^ProcessBuilder pb (ProcessBuilder.
                               ^"[Ljava.lang.String;"
                               (into-array String ["python3" "-c" py-script]))
          _ (.redirectErrorStream pb true)
          proc (.start pb)
          output (str/trim (slurp (.getInputStream proc)))
          _ (.waitFor proc)]
      (println (format "  %-15s n=%,7d d=%2d" dataset-name n d))
      (println (format "    Stratum      AUC=%.4f  train=%s  score=%s"
                       stratum-auc (fmt-ms stratum-train-ms) (fmt-ms stratum-score-ms)))
      (cond
        (str/includes? output "PYOD_NOT_INSTALLED")
        (println "    PyOD         SKIPPED (pip install pyod)")

        (str/includes? output ",")
        (let [[auc train score] (str/split output #",")]
          (println (format "    PyOD         AUC=%.4f  train=%7sms  score=%7sms"
                           (Double/parseDouble auc) (str/trim train) (str/trim score))))

        :else
        (println (format "    PyOD         ERROR: %s" output))))))

(defn- bench-pyod []
  (println "\n=== Stratum vs PyOD Comparison ===")
  (println "  (100 trees, 256 sample size, same seed)")
  (ensure-odds-data)
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
        ^"[[D" features (into-array (Class/forName "[D")
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
