# Anomaly Detection

Stratum includes a built-in isolation forest implementation for out-of-distribution (OOD) detection on columnar data. The algorithm runs entirely in Java with SIMD-accelerated scoring, achieving sub-50ms latency on 6M rows.

## Overview

The isolation forest (Liu et al. 2008) detects anomalies by measuring how easily a point can be isolated from the rest of the data. Anomalous points have shorter average path lengths in random binary trees, yielding higher anomaly scores.

**Key properties:**
- Unsupervised: no labels required
- Linear time: O(N * T * log(psi)) for training, O(N * T * log(psi)) for scoring
- Low memory: ~2.5 MB for 100 trees with sample size 256
- Deterministic: fixed seed produces identical results

## Quick Start

```clojure
(require '[stratum.api :as st])

;; Prepare data
(def amounts (double-array [10 15 12 11 14 200 13 11 300 12]))
(def freqs   (double-array [ 5  6  4  5  7   1  5  4   1  6]))

;; Train with auto-threshold (5% contamination)
(def model (st/train-iforest {:from {:amount amounts :freq freqs}
                              :contamination 0.05}))

;; Score all rows: double[] in [0, 1]
(st/iforest-score model {:amount amounts :freq freqs})

;; Binary prediction: long[] with 1 = anomaly, 0 = normal
(st/iforest-predict model {:amount amounts :freq freqs})
```

## API Reference

### `train-iforest`

Train an isolation forest on columnar data.

```clojure
(st/train-iforest {:from          {:col1 data1 :col2 data2}  ;; required
                   :n-trees       100                        ;; default 100
                   :sample-size   256                        ;; default 256
                   :seed          42                         ;; default 42
                   :contamination 0.05})                     ;; optional (0, 0.5]
```

**Parameters:**
- `:from` - map of keyword to `double[]` or `long[]` columns (required)
- `:n-trees` - number of isolation trees (default 100)
- `:sample-size` - rows subsampled per tree (default 256). Controls tree depth: `ceil(log2(sample-size))`
- `:seed` - random seed for reproducibility
- `:contamination` - expected fraction of anomalies in training data. When set, computes a score threshold automatically from the training score distribution (percentile at `1 - contamination`)

**Returns** a model map containing the flat forest array, metadata, and (if contamination was set) the threshold, training score min/max.

### `iforest-score`

Raw anomaly scores.

```clojure
(st/iforest-score model data)  ;; → double[]
```

Returns `double[]` of anomaly scores in `[0, 1]`. Higher values indicate more anomalous points. Typical anomaly threshold: 0.6-0.7.

### `iforest-predict`

Binary anomaly classification.

```clojure
(st/iforest-predict model data)                   ;; uses auto-threshold
(st/iforest-predict model data {:threshold 0.65}) ;; explicit threshold
```

Returns `long[]` with `1` for anomaly, `0` for normal. Requires either `:contamination` during training or an explicit `:threshold`.

### `iforest-predict-proba`

Calibrated anomaly probability.

```clojure
(st/iforest-predict-proba model data)  ;; → double[]
```

Returns `double[]` in `[0, 1]`. If the model was trained with `:contamination`, normalizes scores using min-max scaling from the training score distribution. Otherwise returns raw scores.

### `iforest-predict-confidence`

Prediction confidence based on tree agreement.

```clojure
(st/iforest-predict-confidence model data)  ;; → double[]
```

Returns `double[]` in `[0, 1]` where `1.0` means all trees fully agree on the point's isolation depth. Uses the coefficient of variation (CV) of per-tree path lengths: `confidence = 1 / (1 + CV)`.

- **High confidence** (>0.8): Trees agree - the prediction is reliable
- **Low confidence** (<0.5): Trees disagree - the point is in an ambiguous region

### `iforest-rotate`

Online model adaptation by replacing oldest trees.

```clojure
(st/iforest-rotate model new-data)      ;; replace 10% of trees (default)
(st/iforest-rotate model new-data 20)   ;; replace 20 trees
```

Returns a new model. The original model is unchanged (CoW semantics).

**Best practices:**
- Rotate with **clean data** (no known anomalies) for best results
- Training on contaminated data reduces detection quality
- Use small `k` (5-10% of n-trees) for gradual adaptation
- Monitor AUC or precision after rotation to validate quality

### `score-weighted`

Scoring with exponential decay weighting for recent trees (useful after rotation).

```clojure
(require '[stratum.iforest :as iforest])
(iforest/score-weighted model data 0.98)  ;; decay=0.98, newer trees weighted higher
```

## Integration with Query Engine

Anomaly scores can be used as regular columns in queries:

```clojure
;; Score, then filter anomalies
(def scores (st/iforest-score model data))
(st/q {:from (assoc data :score scores)
      :where [[:> :score 0.7]]
      :agg [[:count]]})

;; Combine with other analytics
(st/q {:from (assoc data :score scores)
      :group [:region]
      :agg [[:avg :score] [:count]]
      :having [[:> :avg 0.5]]
      :order [[:avg :desc]]})
```

## SQL Interface

Register a model with the pgwire server, then use SQL functions:

```clojure
;; Server setup
(def srv (st/start-server {:port 5432}))
(st/register-table! srv "transactions" tx-data)
(st/register-model! srv "fraud_model" model)
```

```sql
-- Raw anomaly score [0, 1]
SELECT *, ANOMALY_SCORE('fraud_model', amount, freq) AS score
FROM transactions
WHERE ANOMALY_SCORE('fraud_model', amount, freq) > 0.7;

-- Binary prediction (1 = anomaly, 0 = normal)
SELECT *, ANOMALY_PREDICT('fraud_model', amount, freq) AS is_anomaly
FROM transactions;

-- Calibrated probability [0, 1]
SELECT *, ANOMALY_PROBA('fraud_model', amount, freq) AS prob
FROM transactions;

-- Prediction confidence (tree agreement) [0, 1]
SELECT *, ANOMALY_CONFIDENCE('fraud_model', amount, freq) AS conf
FROM transactions;
```

The column arguments must match the feature names used during training (in order).

## Implementation Details

### Tree Representation

Trees are packed into a single `long[]` array for cache-friendly traversal:

- Each internal node stores split feature index (upper 32 bits) and split value as float (lower 32 bits) in one `long`
- Each leaf stores the path length adjustment as `Double.doubleToLongBits(c(leafSize))`
- Trees are laid out contiguously: `forest[t * maxNodes + nodeIdx]`
- Total memory: `n-trees * (2 * sample-size - 1) * 8 bytes` = ~2.5 MB for default settings

### Scoring Algorithm

For each row, traverse each tree from root to leaf. The anomaly score is:

```
score(x) = 2^(-E(h(x)) / c(psi))
```

where `E(h(x))` is the mean path length across all trees and `c(n) = 2*H(n-1) - 2*(n-1)/n` is the expected path length of an unsuccessful BST search (with `H(i)` being the harmonic number).

Scoring is parallelized using morsel-driven execution (64K rows per morsel) across all available cores.

### Contamination and Thresholding

When `:contamination` is set:
1. Score all training data
2. Sort scores
3. Set threshold at the `(1 - contamination)` percentile
4. Record min/max scores for probability calibration

This means `predict` labels approximately `contamination * 100%` of the training data as anomalies.

## Input Validation

All inputs are validated against malli schemas (`stratum.specification`):
- `STrainOpts` validates `:from` is a column map, `:contamination` is in (0, 0.5], etc.
- Invalid inputs produce clear error messages:

```clojure
(st/train-iforest {:from "bad"})
;; => ExceptionInfo Invalid input: {:from ["must be a map of keyword → column data ..."]}
```

## Related Documentation

- [Query Engine](query-engine.md) - Using anomaly scores in queries
- [SQL Interface](sql-interface.md) - SQL anomaly functions
- [Architecture](architecture.md) - System overview
