(ns stratum.iforest-test
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.iforest :as iforest]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.util Random]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Test Data Generators
;; ============================================================================

(defn- generate-gaussian-with-outliers
  "Generate n normal points in d dimensions plus n-outliers at distance from center."
  [n n-outliers d outlier-distance seed]
  (let [rng (Random. seed)
        total (+ n n-outliers)
        features (into-array (Class/forName "[D")
                             (mapv (fn [_]
                                     (let [arr (double-array total)]
                             ;; Normal points: N(0, 1)
                                       (dotimes [i n]
                                         (aset arr i (.nextGaussian rng)))
                             ;; Outliers: far from center
                                       (dotimes [i n-outliers]
                                         (aset arr (+ n i) (+ outlier-distance (* 0.1 (.nextGaussian rng)))))
                                       arr))
                                   (range d)))
        labels (long-array total)]
    (dotimes [i n-outliers]
      (aset labels (+ n i) 1))
    {:features features :labels labels :n total :d d
     :n-normal n :n-outliers n-outliers}))

(defn- auc-roc
  "Compute AUC-ROC from scores and binary labels.
   Uses trapezoidal rule on sorted thresholds."
  [^doubles scores ^longs labels ^long n]
  (let [;; Create index array sorted by score descending
        indices (int-array n)
        _ (dotimes [i n] (aset indices i i))
        ;; Sort by score descending (manual insertion sort — fine for tests)
        pairs (sort-by first > (mapv (fn [i] [(aget scores i) (aget labels i)]) (range n)))
        n-pos (areduce labels i s (long 0) (+ s (aget labels i)))
        n-neg (- n n-pos)]
    (if (or (zero? n-pos) (zero? n-neg))
      Double/NaN
      (loop [tp 0.0, fp 0.0, prev-tp 0.0, prev-fp 0.0, auc 0.0, idx 0]
        (if (>= idx n)
          ;; Final trapezoid
          (let [tpr (/ tp n-pos)
                fpr (/ fp n-neg)
                prev-tpr (/ prev-tp n-pos)
                prev-fpr (/ prev-fp n-neg)]
            (+ auc (* 0.5 (- fpr prev-fpr) (+ tpr prev-tpr))))
          (let [[_ label] (nth pairs idx)
                new-tp (if (== label 1) (inc tp) tp)
                new-fp (if (== label 0) (inc fp) fp)]
            ;; Add trapezoid area when score changes or at end
            (if (or (= idx (dec n))
                    (not= (first (nth pairs idx))
                          (first (nth pairs (min (inc idx) (dec n))))))
              (let [tpr (/ new-tp n-pos)
                    fpr (/ new-fp n-neg)
                    prev-tpr (/ prev-tp n-pos)
                    prev-fpr (/ prev-fp n-neg)
                    area (* 0.5 (- fpr prev-fpr) (+ tpr prev-tpr))]
                (recur new-tp new-fp new-tp new-fp (+ auc area) (inc idx)))
              (recur new-tp new-fp prev-tp prev-fp auc (inc idx)))))))))

;; ============================================================================
;; CSV Data Loading for ODDS Datasets
;; ============================================================================

(defn- load-odds-csv
  "Load an ODDS-format CSV file. Last column is label (0=normal, 1=anomaly).
   All other columns are features (double).
   Returns {:features double[][], :labels long[], :n int, :d int} or nil."
  [^String path]
  (when (.exists (io/file path))
    (let [lines (vec (remove str/blank? (str/split-lines (slurp path))))
          ;; Skip header if first line contains non-numeric chars
          has-header? (try (Double/parseDouble (first (str/split (first lines) #","))) false
                           (catch Exception _ true))
          data-lines (if has-header? (rest lines) lines)
          n (count data-lines)
          first-fields (str/split (first data-lines) #",")
          n-cols (count first-fields)
          d (dec n-cols) ;; last column is label
          features (into-array (Class/forName "[D")
                               (mapv (fn [_] (double-array n)) (range d)))
          labels (long-array n)]
      (doseq [[i line] (map-indexed vector data-lines)]
        (let [fields (str/split line #",")]
          (dotimes [f d]
            (aset ^doubles (aget ^"[[D" features f) i (Double/parseDouble (nth fields f))))
          (aset labels i (long (Double/parseDouble (nth fields d))))))
      {:features features :labels labels :n n :d d})))

;; ============================================================================
;; Core Tests
;; ============================================================================

(deftest train-and-score-basic-test
  (testing "Training and scoring on synthetic data with clear outliers"
    (let [{:keys [features labels n d]} (generate-gaussian-with-outliers 1000 10 5 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
          scores (iforest/score model feature-map)]
      ;; Basic checks
      (is (= n (alength ^doubles scores)))
      (is (every? #(<= 0.0 % 1.0) (seq scores)))
      ;; Outliers should score higher than most normals
      (let [outlier-scores (mapv #(aget ^doubles scores (int %)) (range 1000 1010))
            normal-scores (mapv #(aget ^doubles scores (int %)) (range 0 100))
            avg-outlier (/ (reduce + outlier-scores) 10.0)
            avg-normal (/ (reduce + normal-scores) 100.0)]
        (is (> avg-outlier avg-normal) "Outliers should score higher than normals")
        (is (> avg-outlier 0.55) "Outliers should have high anomaly scores")))))

(deftest determinism-test
  (testing "Fixed seed produces identical scores"
    (let [{:keys [features]} (generate-gaussian-with-outliers 500 5 3 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          m1 (iforest/train {:from feature-map :n-trees 50 :sample-size 128 :seed 42})
          m2 (iforest/train {:from feature-map :n-trees 50 :sample-size 128 :seed 42})
          s1 (iforest/score m1 feature-map)
          s2 (iforest/score m2 feature-map)]
      (is (java.util.Arrays/equals ^doubles s1 ^doubles s2)
          "Same seed should produce identical scores"))))

(deftest auc-synthetic-test
  (testing "AUC > 0.95 on synthetic data with well-separated outliers"
    (let [{:keys [features labels n d]} (generate-gaussian-with-outliers 5000 50 5 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
          scores (iforest/score model feature-map)
          auc (auc-roc scores labels n)]
      (is (> auc 0.95) (format "AUC should be > 0.95, got %.4f" auc)))))

(deftest expected-path-length-test
  (testing "Expected path length values"
    (is (== 0.0 (iforest/expected-path-length 1)))
    (is (== 1.0 (iforest/expected-path-length 2)))
    ;; c(256) ≈ 2*H(255) - 2*255/256 ≈ 2*(ln(255)+0.577) - 1.992 ≈ 10.24
    (is (< 9.0 (iforest/expected-path-length 256) 11.0))))

;; ============================================================================
;; ODDS Dataset Tests (real data)
;; ============================================================================

(deftest odds-shuttle-test
  (testing "Shuttle dataset from ODDS (if available)"
    (when-let [data (load-odds-csv "data/odds/shuttle.csv")]
      (let [{:keys [features labels n d]} data
            feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
            model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
            scores (iforest/score model feature-map)
            auc (auc-roc scores labels n)]
        (println (format "  Shuttle: n=%d, d=%d, AUC=%.4f" n d auc))
        (is (> auc 0.95) (format "Shuttle AUC should be > 0.95, got %.4f" auc))))))

(deftest odds-http-test
  (testing "Http dataset from ODDS (if available)"
    (when-let [data (load-odds-csv "data/odds/http.csv")]
      (let [{:keys [features labels n d]} data
            feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
            model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
            scores (iforest/score model feature-map)
            auc (auc-roc scores labels n)]
        (println (format "  Http: n=%d, d=%d, AUC=%.4f" n d auc))
        (is (> auc 0.95) (format "Http AUC should be > 0.95, got %.4f" auc))))))

(deftest odds-forestcover-test
  (testing "ForestCover dataset from ODDS (if available)"
    (when-let [data (load-odds-csv "data/odds/forestcover.csv")]
      (let [{:keys [features labels n d]} data
            feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
            model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
            scores (iforest/score model feature-map)
            auc (auc-roc scores labels n)]
        (println (format "  ForestCover: n=%d, d=%d, AUC=%.4f" n d auc))
        (is (> auc 0.80) (format "ForestCover AUC should be > 0.80, got %.4f" auc))))))

(deftest odds-mammography-test
  (testing "Mammography dataset from ODDS (if available)"
    (when-let [data (load-odds-csv "data/odds/mammography.csv")]
      (let [{:keys [features labels n d]} data
            feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
            model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
            scores (iforest/score model feature-map)
            auc (auc-roc scores labels n)]
        (println (format "  Mammography: n=%d, d=%d, AUC=%.4f" n d auc))
        (is (> auc 0.80) (format "Mammography AUC should be > 0.80, got %.4f" auc))))))

(deftest odds-creditcard-test
  (testing "Credit Card fraud dataset (if available)"
    (when-let [data (load-odds-csv "data/odds/creditcard.csv")]
      (let [{:keys [features labels n d]} data
            feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
            model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
            scores (iforest/score model feature-map)
            auc (auc-roc scores labels n)]
        (println (format "  CreditCard: n=%d, d=%d, AUC=%.4f" n d auc))
        (is (> auc 0.85) (format "CreditCard AUC should be > 0.85, got %.4f" auc))))))

;; ============================================================================
;; Forest Rotation Tests
;; ============================================================================

(deftest rotate-forest-test
  (testing "Forest rotation produces valid model"
    (let [{:keys [features n]} (generate-gaussian-with-outliers 1000 10 3 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          original (iforest/train {:from feature-map :n-trees 50 :sample-size 128 :seed 42})
          rotated (iforest/rotate-forest original feature-map 5)]
      ;; Same number of trees
      (is (= (:n-trees rotated) (:n-trees original)))
      ;; Forest array same length
      (is (= (alength ^longs (:forest rotated)) (alength ^longs (:forest original))))
      ;; Can still score
      (let [^doubles scores (iforest/score rotated feature-map)]
        (is (= n (alength scores)))
        (is (every? #(<= 0.0 % 1.0) (seq scores)))))))

(deftest rotate-preserves-original-test
  (testing "Rotation does not modify original model (CoW)"
    (let [{:keys [features]} (generate-gaussian-with-outliers 500 5 3 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          original (iforest/train {:from feature-map :n-trees 20 :sample-size 64 :seed 42})
          original-forest (java.util.Arrays/copyOf ^longs (:forest original) (alength ^longs (:forest original)))
          _ (iforest/rotate-forest original feature-map 5)]
      ;; Original forest should be unchanged
      (is (java.util.Arrays/equals ^longs (:forest original) original-forest)
          "Original model should not be modified by rotation"))))

(deftest weighted-scoring-test
  (testing "Weighted scoring produces valid scores"
    (let [{:keys [features n]} (generate-gaussian-with-outliers 500 5 3 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 50 :sample-size 128 :seed 42})
          ;; Equal weight (decay=1.0) should be similar to unweighted
          ^doubles scores-eq (iforest/score-weighted model feature-map 1.0)
          ^doubles scores-normal (iforest/score model feature-map)]
      (is (= n (alength scores-eq)))
      ;; Equal-weight and normal scores should be close (not identical due to different code paths)
      (let [diff (reduce (fn [^double acc ^long i]
                           (+ acc (Math/abs (double (- (aget scores-eq (int i))
                                                       (aget scores-normal (int i)))))))
                         0.0 (range n))
            avg-diff (/ diff n)]
        (is (< avg-diff 0.05) (format "Avg score diff with decay=1.0 should be < 0.05, got %.4f" avg-diff))))))

;; ============================================================================
;; Contamination & Predict Tests
;; ============================================================================

(deftest contamination-test
  (testing "Training with contamination sets threshold"
    (let [{:keys [features labels n]} (generate-gaussian-with-outliers 1000 50 5 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42
                                :contamination 0.05})]
      (is (contains? model :threshold) "Model should have threshold")
      (is (contains? model :contamination) "Model should have contamination")
      (is (contains? model :training-scores-min) "Model should have training score stats")
      (is (number? (:threshold model)))
      (is (< 0.0 (:threshold model) 1.0) "Threshold should be in (0, 1)"))))

(deftest contamination-validation-test
  (testing "Invalid contamination throws"
    (let [feature-map {:f0 (double-array [1 2 3 4 5])}]
      (is (thrown? Exception
                   (iforest/train {:from feature-map :contamination 0.0})))
      (is (thrown? Exception
                   (iforest/train {:from feature-map :contamination 0.6}))))))

(deftest predict-test
  (testing "Binary prediction with contamination"
    (let [{:keys [features labels n]} (generate-gaussian-with-outliers 1000 50 5 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42
                                :contamination 0.05})
          ^longs preds (iforest/predict model feature-map)]
      ;; Should return array of same length
      (is (= n (alength preds)))
      ;; All values should be 0 or 1
      (is (every? #{0 1} (seq preds)))
      ;; Some anomalies should be detected
      (let [n-flagged (areduce preds i s (long 0) (+ s (aget preds i)))]
        (is (> n-flagged 0) "Should detect some anomalies")
        ;; With 5% contamination, ~5% should be flagged
        (is (< n-flagged (* 0.15 n)) "Should not flag more than 15% as anomalies")))))

(deftest predict-explicit-threshold-test
  (testing "Binary prediction with explicit threshold"
    (let [{:keys [features n]} (generate-gaussian-with-outliers 500 10 3 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 50 :sample-size 128 :seed 42})
          ^longs preds (iforest/predict model feature-map {:threshold 0.6})]
      (is (= n (alength preds)))
      (is (every? #{0 1} (seq preds))))))

(deftest predict-no-threshold-throws-test
  (testing "Predict without threshold throws"
    (let [feature-map {:f0 (double-array [1 2 3 4 5])}
          model (iforest/train {:from feature-map :n-trees 10 :sample-size 4 :seed 42})]
      (is (thrown? Exception (iforest/predict model feature-map))))))

(deftest predict-proba-test
  (testing "Predict probability with contamination"
    (let [{:keys [features n]} (generate-gaussian-with-outliers 1000 50 5 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42
                                :contamination 0.05})
          ^doubles proba (iforest/predict-proba model feature-map)]
      (is (= n (alength proba)))
      ;; All probabilities in [0, 1]
      (is (every? #(<= 0.0 % 1.0) (seq proba)))
      ;; Outliers should have higher probability than normals
      (let [outlier-proba (/ (reduce + (map #(aget proba (int %)) (range 1000 1050))) 50.0)
            normal-proba (/ (reduce + (map #(aget proba (int %)) (range 0 100))) 100.0)]
        (is (> outlier-proba normal-proba) "Outlier probability should be higher"))))

  (testing "Predict probability without contamination returns raw scores"
    (let [feature-map {:f0 (double-array [1 2 3 4 5])}
          model (iforest/train {:from feature-map :n-trees 10 :sample-size 4 :seed 42})
          ^doubles proba (iforest/predict-proba model feature-map)
          ^doubles scores (iforest/score model feature-map)]
      (is (java.util.Arrays/equals proba scores)))))

(deftest predict-confidence-test
  (testing "Confidence scores are in [0, 1]"
    (let [{:keys [features n]} (generate-gaussian-with-outliers 1000 50 5 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
          ^doubles conf (iforest/predict-confidence model feature-map)]
      (is (= n (alength conf)))
      ;; All confidence values in [0, 1]
      (is (every? #(<= 0.0 % 1.0) (seq conf)))
      ;; Most confidence values should be reasonably high (trees generally agree)
      (let [avg-conf (/ (areduce conf i s 0.0 (+ s (aget conf i))) n)]
        (is (> avg-conf 0.3) (format "Average confidence should be > 0.3, got %.4f" avg-conf))))))

(deftest score-detailed-test
  (testing "Score-detailed returns scores and variance"
    (let [{:keys [features n]} (generate-gaussian-with-outliers 500 10 3 10.0 42)
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 50 :sample-size 128 :seed 42})
          result (iforest/score-detailed model feature-map)
          ^doubles scores (:scores result)
          ^doubles var (:path-length-variance result)]
      ;; Both arrays present and correct length
      (is (= n (alength scores)))
      (is (= n (alength var)))
      ;; Scores match regular scoring
      (let [^doubles regular (iforest/score model feature-map)]
        (dotimes [i n]
          (is (< (Math/abs (- (aget scores i) (aget regular i))) 1e-10)
              "Detailed scores should match regular scores")))
      ;; Variances should be non-negative
      (is (every? #(>= % 0.0) (seq var))))))

;; ============================================================================
;; Performance Smoke Test
;; ============================================================================

(deftest scoring-performance-test
  (testing "Scoring 100K rows in reasonable time"
    (let [n 100000
          d 5
          rng (Random. 42)
          features (into-array (Class/forName "[D")
                               (mapv (fn [_]
                                       (let [arr (double-array n)]
                                         (dotimes [i n] (aset arr i (.nextGaussian rng)))
                                         arr))
                                     (range d)))
          feature-map (into {} (map-indexed (fn [i col] [(keyword (str "f" i)) col]) features))
          model (iforest/train {:from feature-map :n-trees 100 :sample-size 256 :seed 42})
          ;; Warmup
          _ (iforest/score model feature-map)
          _ (iforest/score model feature-map)
          ;; Timed run
          t0 (System/nanoTime)
          ^doubles scores (iforest/score model feature-map)
          elapsed-ms (/ (- (System/nanoTime) t0) 1e6)]
      (is (= n (alength scores)))
      (println (format "  IForest scoring: %d rows × 100 trees in %.1fms" n elapsed-ms))
      ;; Should be well under 1 second for 100K rows
      (is (< elapsed-ms 5000) (format "Scoring should be < 5s, took %.1fms" elapsed-ms)))))
