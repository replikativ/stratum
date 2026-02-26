(ns stratum.iforest
  "Isolation Forest anomaly detection for Stratum.

   Provides training, scoring, prediction, and online rotation of
   isolation forests (Liu et al. 2008).

   Usage:
     ;; Train a model (optionally with contamination for auto-threshold)
     (def model (train {:from {:amount amounts :freq freqs}
                        :n-trees 100 :sample-size 256 :seed 42
                        :contamination 0.05}))

     ;; Raw anomaly scores [0, 1] — higher = more anomalous
     (def scores (score model {:amount amounts :freq freqs}))

     ;; Binary prediction: 1 = anomaly, 0 = normal
     (def labels (predict model {:amount amounts :freq freqs}))

     ;; Anomaly probability [0, 1] (normalized via training distribution)
     (def proba (predict-proba model {:amount amounts :freq freqs}))

     ;; Confidence per prediction (tree agreement)
     (def conf (predict-confidence model {:amount amounts :freq freqs}))

   Integration with query engine:
     (q/execute {:from {:amount amounts :freq freqs :score (score model data)}
                 :where [[:> :score 0.7]]
                 :agg [[:count]]})"
  (:require [stratum.specification :as spec])
  (:import [stratum.internal ColumnOpsAnalytics]))

(set! *warn-on-reflection* true)

(defn- prepare-features
  "Extract double[][] features array from a column map, converting long[] → double[] as needed."
  [from feature-names]
  (into-array (Class/forName "[D")
              (mapv (fn [col-name]
                      (let [col (get from col-name)]
                        (cond
                          (instance? (Class/forName "[D") col) col
                          (instance? (Class/forName "[J") col)
                          (let [^longs la col
                                da (double-array (alength la))]
                            (dotimes [i (alength la)]
                              (aset da i (double (aget la i))))
                            da)
                          :else (throw (ex-info "Column must be double[] or long[]"
                                                {:col col-name})))))
                    feature-names)))

(defn- count-rows
  "Get row count from first column of a data map."
  ^long [from feature-names]
  (let [first-col (get from (first feature-names))]
    (cond
      (instance? (Class/forName "[D") first-col) (alength ^doubles first-col)
      (instance? (Class/forName "[J") first-col) (alength ^longs first-col)
      :else (throw (ex-info "Columns must be double[] or long[]" {:col (first feature-names)})))))

(defn score
  "Score rows using a trained isolation forest.

   Returns double[n-rows] of anomaly scores in [0, 1].
   Higher scores indicate more anomalous points.
   Typical threshold: 0.6-0.7 for anomaly detection.

   model    — trained model from `train`
   data     — map of keyword → double[] columns (same features as training)"
  [model data]
  (let [{:keys [forest n-trees sample-size feature-names n-features]} model
        n-rows (count-rows data feature-names)
        features (prepare-features data feature-names)]
    (ColumnOpsAnalytics/iforestScoreParallel ^longs forest (int n-trees) (int sample-size)
                                             features (int n-rows) (int n-features))))

(defn expected-path-length
  "Expected path length of unsuccessful BST search (utility, exposed for testing)."
  ^double [^long n]
  (ColumnOpsAnalytics/expectedPathLength (int n)))

(defn train
  "Train an isolation forest on columnar data.

   Options:
     :from          — map of keyword → double[]/long[] columns (required)
     :n-trees       — number of trees (default 100)
     :sample-size   — subsample size per tree (default 256)
     :seed          — random seed for reproducibility (default 42)
     :contamination — expected fraction of anomalies in training data (0, 0.5].
                      Sets threshold automatically from training score distribution.
                      If not set, use `predict` with explicit :threshold.

   Returns a model map:
     {:forest long[], :n-trees int, :sample-size int,
      :feature-names [keywords], :n-features int,
      :threshold double (if contamination set),
      :contamination double (if set)}"
  [{:keys [from n-trees sample-size seed contamination]
    :or {n-trees 100 sample-size 256 seed 42}
    :as opts}]
  (spec/validate! spec/STrainOpts opts {:op :train})
  (let [feature-names (vec (keys from))
        n-features (count feature-names)
        n-rows (count-rows from feature-names)
        features (prepare-features from feature-names)
        forest (ColumnOpsAnalytics/iforestTrain features n-rows n-features
                                                (int n-trees) (int sample-size) (long seed))
        model {:forest forest
               :n-trees n-trees
               :sample-size sample-size
               :feature-names feature-names
               :n-features n-features}]
    (if contamination
      (let [c (double contamination)
            _ (when (or (<= c 0.0) (> c 0.5))
                (throw (ex-info "contamination must be in (0, 0.5]" {:contamination c})))
            ^doubles scores (score model from)
            n (alength scores)
            sorted (java.util.Arrays/copyOf scores n)
            _ (java.util.Arrays/sort sorted)
            ;; Threshold: score at (1 - contamination) percentile
            idx (min (dec n) (int (* (- 1.0 c) (dec n))))
            threshold (aget sorted idx)]
        (assoc model
               :contamination contamination
               :threshold threshold
               :training-scores-min (aget sorted 0)
               :training-scores-max (aget sorted (dec n))))
      model)))

(defn predict
  "Binary anomaly prediction.
   Returns long[] with 1 for anomaly, 0 for normal.

   Uses threshold from :contamination (if set during training),
   or pass {:threshold 0.65} as options."
  ([model data] (predict model data {}))
  ([model data {:keys [threshold]}]
   (let [t (double (or threshold
                       (:threshold model)
                       (throw (ex-info "No threshold set. Train with :contamination or pass {:threshold t}"
                                       {:model-keys (keys model)}))))
         ^doubles scores (score model data)
         n (alength scores)
         result (long-array n)]
     (dotimes [i n]
       (when (> (aget scores i) t)
         (aset result i 1)))
     result)))

(defn predict-proba
  "Anomaly probability for each row.
   Returns double[] in [0, 1] where higher = more likely anomalous.

   If model was trained with :contamination, normalizes scores using
   the training score distribution (min-max). Otherwise returns raw scores."
  [model data]
  (let [^doubles scores (score model data)]
    (if-let [smin (:training-scores-min model)]
      (let [smin (double smin)
            smax (double (:training-scores-max model))
            range (- smax smin)
            n (alength scores)
            proba (double-array n)]
        (if (< range 1e-10)
          (do (java.util.Arrays/fill proba 0.5) proba)
          (do (dotimes [i n]
                (aset proba i (max 0.0 (min 1.0 (/ (- (aget scores i) smin) range)))))
              proba)))
      ;; No training reference — return raw scores
      scores)))

(defn score-detailed
  "Score with detailed diagnostics per row.
   Returns {:scores double[], :path-length-variance double[]}.

   Path length variance indicates tree agreement:
   - Low variance = trees agree on this point's depth → high confidence
   - High variance = trees disagree → uncertain classification"
  [model data]
  (let [{:keys [forest n-trees sample-size feature-names n-features]} model
        n-rows (count-rows data feature-names)
        features (prepare-features data feature-names)
        ^"[[D" result (ColumnOpsAnalytics/iforestScoreAndVarianceParallel
                       ^longs forest (int n-trees) (int sample-size)
                       features (int n-rows) (int n-features))]
    {:scores (aget result 0)
     :path-length-variance (aget result 1)}))

(defn predict-confidence
  "Confidence of anomaly prediction for each row.
   Returns double[] in [0, 1] where 1 = all trees fully agree.

   Uses coefficient of variation of per-tree path lengths:
   confidence = 1 / (1 + CV) where CV = stddev / mean.

   High confidence means trees agree on this point's isolation depth.
   Low confidence means the model is uncertain about this point."
  [model data]
  (let [{:keys [scores path-length-variance]} (score-detailed model data)
        ^doubles scores scores
        ^doubles variances path-length-variance
        n (alength scores)
        sample-size (:sample-size model)
        c-psi (expected-path-length sample-size)
        conf (double-array n)]
    (dotimes [i n]
      (let [s (aget scores i)
            ;; Recover mean path length: score = 2^(-mean/c_psi) → mean = -c_psi * log2(score)
            mean-pl (if (> s 1e-15)
                      (- (* c-psi (/ (Math/log s) (Math/log 2.0))))
                      (* c-psi 10.0)) ;; very high score → very short path → use large mean
            stddev (Math/sqrt (Math/max 0.0 (aget variances i)))
            cv (if (> mean-pl 1e-10) (/ stddev mean-pl) 0.0)]
        (aset conf i (/ 1.0 (+ 1.0 cv)))))
    conf))

(defn rotate-forest
  "Replace the oldest `k` trees with new trees trained on `new-data`.
   Returns a new model (the original is unchanged — structural sharing via CoW).

   Best practices for online rotation:
   - Rotate with CLEAN data (no known anomalies) for best results
   - Training on contaminated data can reduce detection quality
   - Use small k (5-10% of n-trees) for gradual adaptation
   - Monitor AUC or precision after rotation to validate quality

   model    — existing trained model
   new-data — map of keyword → double[] columns for training new trees
   k        — number of trees to replace (default: 10% of n-trees)"
  ([model new-data] (rotate-forest model new-data (max 1 (quot (:n-trees model) 10))))
  ([model new-data k]
   (let [{:keys [forest n-trees sample-size]} model
         max-nodes (dec (* 2 sample-size))
         tree-size max-nodes  ;; packed: 1 long per node
         new-model (train {:from new-data
                           :n-trees k
                           :sample-size sample-size
                           :seed (System/nanoTime)})
         ^longs new-forest (:forest new-model)
         ^longs old-forest forest
         rotated (long-array (alength old-forest))]
     ;; Copy trees [k..n-trees) from old forest to positions [0..n-trees-k)
     (System/arraycopy old-forest (* k tree-size)
                       rotated 0
                       (* (- n-trees k) tree-size))
     ;; Copy new trees to positions [n-trees-k..n-trees)
     (System/arraycopy new-forest 0
                       rotated (* (- n-trees k) tree-size)
                       (* k tree-size))
     ;; Preserve threshold/contamination from original if set
     (cond-> (assoc model :forest rotated)
       (:threshold model) (assoc :threshold (:threshold model))))))

(defn score-weighted
  "Score with exponential decay weighting favoring recent trees.

   decay — weight decay factor per tree age (1.0 = equal, 0.99 = slight recency bias)
   Tree 0 is oldest, tree n-1 is newest."
  [model data ^double decay]
  (let [{:keys [forest n-trees sample-size feature-names n-features]} model
        n-rows (count-rows data feature-names)
        features (prepare-features data feature-names)
        max-nodes (dec (* 2 sample-size))
        max-depth (int (Math/ceil (/ (Math/log sample-size) (Math/log 2))))
        ^longs forest-arr forest
        c-psi (expected-path-length sample-size)
        scores (double-array n-rows)
        weights (double-array n-trees)]
    (dotimes [t n-trees]
      (aset weights t (Math/pow decay (double (- (dec n-trees) t)))))
    (let [weight-sum (areduce weights i s 0.0 (+ s (aget weights i)))]
      (dotimes [i n-rows]
        (let [total-path (loop [t 0, acc 0.0]
                           (if (>= t n-trees)
                             acc
                             (let [tree-offset (int (* t max-nodes))
                                   ;; Branchless traversal with packed nodes
                                   node (loop [node (int 0), d (int 0)]
                                          (if (>= d max-depth)
                                            node
                                            (let [packed (aget forest-arr (+ tree-offset node))
                                                  feat (int (unsigned-bit-shift-right packed 32))
                                                  split-val (Float/intBitsToFloat (unchecked-int packed))
                                                  val (aget ^doubles (aget ^"[[D" features (Math/max (int feat) (int 0))) i)
                                                  cmp (if (>= val (double split-val)) 1 0)]
                                              (recur (int (+ 1 (* 2 node) cmp)) (int (inc d))))))
                                   ;; Leaf: long bits → double path length
                                   pl (Double/longBitsToDouble (aget forest-arr (+ tree-offset node)))]
                               (recur (inc t) (+ acc (* (aget weights t) pl))))))]
          (aset scores i (Math/pow 2.0 (- (/ total-path (* weight-sum c-psi)))))))
      scores)))
