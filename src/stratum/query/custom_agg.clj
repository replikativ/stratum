(ns stratum.query.custom-agg
  "Open-monoid grouped fold — CUSTOM aggregation for the columnar engine.

   Stratum's built-in aggregations are a closed set dispatched by `case` in group_by.clj
   (#{:sum :count :min :max :avg :sum-product :variance :stddev :corr :count-distinct}), each with a
   hardcoded SIMD kernel. This namespace ADDS an open seam: any associative `{unit, combine, finalize}`
   monoid runs per group, reusing the dense-code grouping model (group index = a dense code 0..G-1, flat
   Object[] accumulators indexed by code) that makes the engine's grouping fast. The accumulate is scalar
   (no per-op SIMD for the custom op), but the grouping and the parallelism are the engine's, and the
   `combine`'s ASSOCIATIVITY is exactly what licenses the parallel partition+merge.

   This is the substrate that lets a verified external monoid (e.g. wandler's kernel-checked reducers)
   ride stratum's columnar grouping for aggregates outside the built-in set. The caller supplies dense
   group codes (0..G-1) and a lifted value column (each value already in the accumulator domain)."
  (:import [java.util ArrayList]))

(defn grouped-fold
  "Fold `vals` (Object[], each already lifted into the accumulator domain) into G groups by `codes`
   (long[] of dense codes 0..G-1), using monoid `m` = {:unit (fn []->acc), :combine (acc acc -> acc),
   :finalize (acc -> result, default identity), :threads N}. Returns an Object[G] of finalized results
   indexed by group code.

   With :threads > 1, the rows are partitioned into N ranges folded concurrently into per-range partial
   accumulator arrays, then merged per group via `combine` — correct because `combine` is associative
   (the monoid law is the certificate). Dense Object[G] partials, so use for G within the dense regime."
  [^longs codes ^objects vals ^long n-groups {:keys [unit combine finalize threads]
                                              :or {finalize identity threads 1}}]
  (let [n (alength codes)
        fold-range (fn [^long lo ^long hi]
                     (let [accs (object-array n-groups)]
                       (dotimes [g n-groups] (aset accs g (unit)))
                       (loop [i lo]
                         (when (< i hi)
                           (let [g (aget codes i)]
                             (aset accs g (combine (aget accs g) (aget vals i))))
                           (recur (inc i))))
                       accs))
        ^objects accs
        (if (<= (long threads) 1)
          (fold-range 0 n)
          (let [p     (int threads)
                chunk (long (Math/ceil (/ (double n) p)))
                parts (mapv (fn [t] (future (fold-range (* (long t) chunk)
                                                        (min n (* (inc (long t)) chunk)))))
                            (range p))
                parts (mapv deref parts)
                merged (object-array n-groups)]
            (dotimes [g n-groups]
              (aset merged g (reduce (fn [a ^objects part] (combine a (aget part g)))
                                     (unit) parts)))
            merged))
        out (object-array n-groups)]
    (dotimes [g n-groups] (aset out g (finalize (aget accs g))))
    out))

(defn grouped-fold-prim-long
  "Primitive (boxing-free) grouped fold for monoids over LONG values — Layer A of the morsel-fold path.
   Dense `long[]` accumulators scattered via a `java.util.function.LongBinaryOperator combine` + a long
   `identity`; parallel partition+merge (the same `combine`, correct because it is associative). Unlike
   `grouped-fold` (Object accumulators + IFn → boxing/allocation per element), this mutates a `long[]`
   with a typed callback the JIT can inline — so a verified numeric monoid runs at near-built-in speed
   while staying an OPEN op outside stratum's closed agg set. Returns a `long[]` indexed by group code."
  [^longs codes ^longs vals n-groups identity
   ^java.util.function.LongBinaryOperator combine threads]
  (let [n         (alength codes)
        n-groups  (long n-groups)
        identity  (long identity)
        threads   (long threads)
        fold-range (fn ^longs [^long lo ^long hi]
                     (let [accs (long-array n-groups identity)]
                       (loop [i lo]
                         (when (< i hi)
                           (let [g (aget codes i)]
                             (aset accs g (.applyAsLong combine (aget accs g) (aget vals i))))
                           (recur (inc i))))
                       accs))]
    (if (<= threads 1)
      (fold-range 0 n)
      (let [p     (int threads)
            chunk (long (Math/ceil (/ (double n) p)))
            parts (mapv (fn [t] (future (fold-range (* (long t) chunk)
                                                    (min n (* (inc (long t)) chunk)))))
                        (range p))
            parts (mapv deref parts)
            merged (long-array n-groups identity)]
        (dotimes [g n-groups]
          (let [m (long (reduce (fn [^long a ^longs part] (.applyAsLong combine a (aget part g)))
                                identity parts))]
            (aset merged g m)))
        merged))))

(defn dense-code-columns
  "Convenience: encode a Clojure seq of keys into dense codes 0..G-1 (first-seen order) + a code->key
   decoder vector. Mirrors how the engine's dense path numbers groups; lets callers that hold row-shaped
   keys produce the (codes, G) grouped-fold expects."
  [keys]
  (let [code (java.util.HashMap.) decode (ArrayList.) n (count keys)
        arr (long-array n) i (int-array 1)]
    (doseq [k keys]
      (let [c (or (.get code k) (let [c (.size decode)] (.put code k c) (.add decode k) c))]
        (aset arr (aget i 0) (long c)) (aset i 0 (inc (aget i 0)))))
    {:codes arr :n-groups (.size decode) :decode (vec decode)}))
