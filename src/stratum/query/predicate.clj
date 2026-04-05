(ns stratum.query.predicate
  "Predicate compilation, splitting, and materialization.

   Covers three concerns:
     1. SIMD eligibility — which predicates the Java SIMD path can handle natively
     2. Compiled predicate mask — non-SIMD predicates compiled to JVM bytecode via eval
     3. String predicate materialization — LIKE/ILIKE/contains evaluated to long[] masks
     4. Strategy selection — simd-eligible? / multi-agg-simd-eligible?"
  (:import [stratum.internal ColumnOps ColumnOpsExt ColumnOpsString]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Compiled Predicate Mask (bytecode-compiled non-SIMD predicates)
;; ============================================================================

(def simd-ops
  "Predicate ops that the Java SIMD path can handle natively."
  #{:lt :gt :lte :gte :eq :neq :range :not-range})

(defn simd-pred?
  "Check if a single normalized predicate can be handled by the SIMD path."
  [pred columns]
  (let [col (first pred)
        op (second pred)]
    (and (keyword? col)
         (contains? simd-ops op)
         (contains? columns col))))

(defn rewrite-null-preds
  "Rewrite IS-NULL/IS-NOT-NULL on long columns to SIMD-native EQ/NEQ.
   Long NULL sentinel is Long/MIN_VALUE, so is-not-null → neq MIN_VALUE.
   Double columns stay on the compiled mask path (NaN != NaN in IEEE754)."
  [preds columns]
  (mapv (fn [pred]
          (let [col (first pred)
                op  (second pred)]
            (if (and (keyword? col)
                     (or (= op :is-null) (= op :is-not-null))
                     (= :int64 (:type (get columns col))))
              (if (= op :is-null)
                [col :eq (double Long/MIN_VALUE)]
                [col :neq (double Long/MIN_VALUE)])
              pred)))
        preds))

(defn split-preds
  "Split predicates into [simd-preds non-simd-preds].
   SIMD preds go to Java. Non-SIMD preds get compiled to a mask."
  [preds columns]
  (let [preds (rewrite-null-preds preds columns)
        groups (group-by #(simd-pred? % columns) preds)]
    [(vec (get groups true []))
     (vec (get groups false []))]))

(defn pred->code
  "Generate Clojure code for a single normalized predicate.
   Returns a form that evaluates to boolean given a bound `i` symbol.
   Column data arrays are captured as closed-over locals."
  [pred col-syms columns]
  (let [col (first pred)
        op (second pred)]
    (case op
      ;; OR: recursively compile sub-predicates
      :or
      (let [sub-preds (subvec pred 2)]
        `(or ~@(mapv #(pred->code % col-syms columns) sub-preds)))

      ;; IN: unroll equality checks (NULL values filtered — x=NULL is always UNKNOWN)
      :in
      (let [vals (filterv some? (nth pred 2))
            col-info (get columns col)
            sym (get col-syms col)
            long-col? (= :int64 (:type col-info))]
        (if (empty? vals)
          `false
          (if (and (:dict col-info) (some string? vals))
            ;; Dict-encoded string column: look up string → code
            (let [dict ^"[Ljava.lang.String;" (:dict col-info)
                  dict-map (into {} (for [i (range (alength dict))] [(aget dict i) (long i)]))
                  code-vals (sort (keep #(get dict-map %) vals))]
              (if (seq code-vals)
                `(let [~'v (aget ~sym (int ~'i))]
                   (or ~@(mapv (fn [v] `(== ~'v ~v)) code-vals)))
                `false))
            (if long-col?
              `(let [~'v (aget ~sym (int ~'i))]
                 (or ~@(mapv (fn [v] `(== ~'v ~(long v))) (sort vals))))
              `(let [~'v (aget ~sym (int ~'i))]
                 (or ~@(mapv (fn [v] `(== ~'v ~(double v))) (sort vals))))))))

      ;; NOT-IN: negate IN
      ;; SQL three-valued logic: NOT IN (1, NULL) is UNKNOWN for all rows.
      ;; If NULL is present in the list, no row can satisfy NOT IN → always false.
      :not-in
      (let [raw-vals (nth pred 2)
            has-null? (some nil? raw-vals)
            vals (filterv some? raw-vals)
            col-info (get columns col)
            sym (get col-syms col)
            long-col? (= :int64 (:type col-info))]
        (if has-null?
          `false ;; NOT IN with NULL → always UNKNOWN → no match
          (if (empty? vals)
            `true
            (if (and (:dict col-info) (some string? vals))
              ;; Dict-encoded string column: look up string → code
              (let [dict ^"[Ljava.lang.String;" (:dict col-info)
                    dict-map (into {} (for [i (range (alength dict))] [(aget dict i) (long i)]))
                    code-vals (sort (keep #(get dict-map %) vals))]
                (if (seq code-vals)
                  `(let [~'v (aget ~sym (int ~'i))]
                     (and ~@(mapv (fn [v] `(not= ~'v ~v)) code-vals)))
                  `true))
              (if long-col?
                `(let [~'v (aget ~sym (int ~'i))]
                   (and ~@(mapv (fn [v] `(not= ~'v ~(long v))) (sort vals))))
                `(let [~'v (aget ~sym (int ~'i))]
                   (and ~@(mapv (fn [v] `(not= ~'v ~(double v))) (sort vals)))))))))

      ;; Simple comparison ops (fallback for preds on missing columns etc.)
      (:lt :gt :lte :gte :eq :neq :range :not-range)
      (let [sym (get col-syms col)
            long-col? (= :int64 (:type (get columns col)))
            args (subvec pred 2)]
        (if long-col?
          (let [v-expr `(aget ~sym (int ~'i))]
            (case op
              :lt        `(< ~v-expr ~(long (first args)))
              :gt        `(> ~v-expr ~(long (first args)))
              :lte       `(<= ~v-expr ~(long (first args)))
              :gte       `(>= ~v-expr ~(long (first args)))
              :eq        `(== ~v-expr ~(long (first args)))
              :neq       `(not= ~v-expr ~(long (first args)))
              :range     `(let [~'v ~v-expr] (and (>= ~'v ~(long (first args))) (<= ~'v ~(long (second args)))))
              :not-range `(let [~'v ~v-expr] (or (< ~'v ~(long (first args))) (> ~'v ~(long (second args)))))))
          (let [v-expr `(aget ~sym (int ~'i))]
            (case op
              :lt        `(< ~v-expr ~(double (first args)))
              :gt        `(> ~v-expr ~(double (first args)))
              :lte       `(<= ~v-expr ~(double (first args)))
              :gte       `(>= ~v-expr ~(double (first args)))
              :eq        `(== ~v-expr ~(double (first args)))
              :neq       `(not= ~v-expr ~(double (first args)))
              :range     `(let [~'v ~v-expr] (and (>= ~'v ~(double (first args))) (<= ~'v ~(double (second args)))))
              :not-range `(let [~'v ~v-expr] (or (< ~'v ~(double (first args))) (> ~'v ~(double (second args)))))))))

      ;; NULL predicates
      :is-null
      (let [sym (get col-syms col)
            long-col? (= :int64 (:type (get columns col)))]
        (if long-col?
          `(== (aget ~sym (int ~'i)) Long/MIN_VALUE)
          `(Double/isNaN (aget ~sym (int ~'i)))))

      :is-not-null
      (let [sym (get col-syms col)
            long-col? (= :int64 (:type (get columns col)))]
        (if long-col?
          `(not= (aget ~sym (int ~'i)) Long/MIN_VALUE)
          `(not (Double/isNaN (aget ~sym (int ~'i))))))

      ;; Function predicate: [:col :fn f :fn-sym sym]
      ;; The fn object is bound to the symbol at index 3 of the pred vector
      ;; by compile-pred-mask (appended during compilation).
      ;; Uses primitive IFn$LO/IFn$DO when the fn supports it (type-hinted arg),
      ;; falls back to boxed IFn.invoke otherwise.
      :fn
      (let [pred-fn (nth pred 2)
            fn-sym (nth pred 3)
            sym (get col-syms col)
            long-col? (= :int64 (:type (get columns col)))
            has-prim? (if long-col?
                        (instance? clojure.lang.IFn$LO pred-fn)
                        (instance? clojure.lang.IFn$DO pred-fn))]
        (if has-prim?
          (if long-col?
            `(.invokePrim ~(with-meta fn-sym {:tag 'clojure.lang.IFn$LO}) (aget ~sym (int ~'i)))
            `(.invokePrim ~(with-meta fn-sym {:tag 'clojure.lang.IFn$DO}) (aget ~sym (int ~'i))))
          `(~fn-sym (aget ~sym (int ~'i)))))

      ;; Unknown op — throw at compile time
      (throw (ex-info (str "Cannot compile predicate op: " op) {:pred pred})))))

(defn compile-pred-mask
  "Compile non-SIMD predicates into a vectorized (fn [^long length] -> long[]) via eval.
   Generates JVM bytecode with the loop inside the compiled code, so C2 JIT can
   optimize the entire mask materialization as one unit — eliminates 1M IFn.invoke
   calls compared to the per-row approach.
   Arrays are passed via a closure over an object array (not embedded in code).
   Supports :fn predicates — function objects are passed in the same object array.
   Returns nil if no non-SIMD predicates."
  [non-simd-preds columns]
  (when (seq non-simd-preds)
    (let [;; Collect all column keywords referenced (recursively for OR)
          all-cols (atom #{})
          _ (letfn [(walk-pred [p]
                      (let [op (second p)]
                        (case op
                          :or (doseq [sub (subvec p 2)] (walk-pred sub))
                          :fn (swap! all-cols conj (first p))
                          (:in :not-in) (swap! all-cols conj (first p))
                          (when (keyword? (first p)) (swap! all-cols conj (first p))))))]
              (doseq [p non-simd-preds] (walk-pred p)))
          needed-cols (vec (filterv #(contains? columns %) @all-cols))

          ;; Create unique symbols for each column
          col-syms (into {} (map (fn [k] [k (gensym (str (name k) "_"))]) needed-cols))

          ;; Collect :fn predicates — assign gensyms and annotate preds with them
          fn-preds (filterv #(= :fn (second %)) non-simd-preds)
          fn-syms (mapv (fn [_] (gensym "pred_fn_")) fn-preds)
          fn-pred-sym-map (zipmap (mapv #(nth % 2) fn-preds) fn-syms)  ;; fn-obj → sym

          ;; Annotate :fn preds with their bound symbol (appended at index 3)
          annotated-preds (mapv (fn [p]
                                  (if (= :fn (second p))
                                    (conj (vec p) (get fn-pred-sym-map (nth p 2)))
                                    p))
                                non-simd-preds)

          ;; Build the code that extracts arrays from an object array parameter
          ;; First: column data arrays (indices 0..n-1)
          ;; Then: fn objects (indices n..n+m-1)
          n-cols (count needed-cols)
          extract-bindings (vec (concat
                                 (mapcat
                                   (fn [idx k]
                                     (let [sym (col-syms k)
                                           col-info (get columns k)]
                                       (if (= :int64 (:type col-info))
                                         [(with-meta sym {:tag 'longs}) `(aget ~'cols ~idx)]
                                         [(with-meta sym {:tag 'doubles}) `(aget ~'cols ~idx)])))
                                   (range) needed-cols)
                                 (mapcat
                                   (fn [idx fn-sym]
                                     [(with-meta fn-sym {:tag 'clojure.lang.IFn})
                                      `(aget ~'cols ~(+ n-cols idx))])
                                   (range) fn-syms)))

          ;; Generate predicate code (using annotated preds with fn-syms)
          pred-codes (mapv #(pred->code % col-syms columns) annotated-preds)

          ;; Compile entire loop: (fn [^objects cols ^long len] -> long[])
          ;; Loop is inside compiled code so JIT optimizes it as one unit
          compiled-fn (eval `(fn [~(with-meta 'cols {:tag 'objects}) ~(with-meta 'len {:tag 'long})]
                               (let [~(with-meta 'mask {:tag 'longs}) (long-array ~'len)
                                     ~@extract-bindings]
                                 (dotimes [~'i ~'len]
                                   (aset ~'mask ~'i (long (if (and ~@pred-codes) 1 0))))
                                 ~'mask)))

          ;; Build the object array: column data arrays + fn objects
          col-data-arr (object-array
                         (concat (mapv #(:data (get columns %)) needed-cols)
                                 (mapv #(nth % 2) fn-preds)))]

      ;; Return (fn [^long length] -> long[]) that closes over the data
      (fn [^long length] ^longs (compiled-fn col-data-arr length)))))

;; ============================================================================
;; String Predicate Materialization
;; ============================================================================

(def string-pred-ops
  "Predicate ops that operate on string columns."
  #{:like :not-like :ilike :not-ilike :contains :starts-with :ends-with})

(defn materialize-string-preds
  "Pre-compute string predicates into long[] mask columns.
   Replaces [:like :col pattern] with [:__like_0 :eq 1].
   Returns [updated-preds updated-columns]."
  [preds columns ^long length]
  (loop [i 0
         out-preds []
         out-cols columns
         remaining preds]
    (if (empty? remaining)
      [out-preds out-cols]
      (let [pred (first remaining)
            op (second pred)]
        (if (string-pred-ops op)
          (let [col-key (first pred)
                col-info (get columns col-key)
                pattern (nth pred 2)
                mask-name (keyword (str "__str_" i))
                mask (cond
                       ;; Dict-encoded string column
                       (and (:dict col-info) (= :string (:dict-type col-info)))
                       (let [codes ^longs (:data col-info)
                             dict ^"[Ljava.lang.String;" (:dict col-info)
                             alpha-masks ^ints (:dict-alpha-masks col-info)
                             bigram-masks ^longs (:dict-bigram-masks col-info)]
                         (case op
                           :like (ColumnOpsString/arrayStringLikeFastMasked codes dict (str pattern) (int length) alpha-masks bigram-masks)
                           :not-like (let [m (ColumnOpsString/arrayStringLikeFastMasked codes dict (str pattern) (int length) alpha-masks bigram-masks)
                                           r (long-array length)]
                                       (dotimes [j length] (aset r j (if (zero? (aget m j)) 1 0)))
                                       r)
                           :ilike (let [ldict (into-array String (map #(.toLowerCase ^String %) dict))]
                                    (ColumnOpsString/arrayStringLikeFastMasked codes ldict (.toLowerCase (str pattern)) (int length) alpha-masks bigram-masks))
                           :not-ilike (let [ldict (into-array String (map #(.toLowerCase ^String %) dict))
                                            m (ColumnOpsString/arrayStringLikeFastMasked codes ldict (.toLowerCase (str pattern)) (int length) alpha-masks bigram-masks)
                                            r (long-array length)]
                                        (dotimes [j length] (aset r j (if (zero? (aget m j)) 1 0)))
                                        r)
                           :contains (ColumnOpsString/arrayStringLikeFastMasked codes dict (str "%" pattern "%") (int length) alpha-masks bigram-masks)
                           :starts-with (ColumnOpsString/arrayStringLikeFastMasked codes dict (str pattern "%") (int length) alpha-masks bigram-masks)
                           :ends-with (ColumnOpsString/arrayStringLikeFastMasked codes dict (str "%" pattern) (int length) alpha-masks bigram-masks)))
                       ;; Not dict-encoded — cannot handle as string pred
                       :else
                       (throw (ex-info (str "String predicate requires dict-encoded column. Use encode-column first.")
                                       {:col col-key :op op})))]
            (recur (inc i)
                   (conj out-preds [mask-name :eq 1])
                   (assoc out-cols mask-name {:type :int64 :data mask})
                   (rest remaining)))
          (recur i (conj out-preds pred) out-cols (rest remaining)))))))

(def dict-resolvable-ops
  "Predicate ops where string args can be resolved to dict codes."
  #{:eq :neq})

(defn resolve-dict-equality-preds
  "Resolve string equality/inequality predicates on dict-encoded columns.
   Converts [:col :eq \"Alice\"] to [:col :eq 0] (using dict code).
   If string is not in dict, :eq becomes impossible (mask=0), :neq is always true (removed).
   Returns updated predicates vector."
  [preds columns]
  (reduce
   (fn [out pred]
     (let [col-key (first pred)
           op (second pred)]
       (if (and (dict-resolvable-ops op)
                (keyword? col-key)
                (>= (count pred) 3)
                (string? (nth pred 2)))
         (let [col-info (get columns col-key)]
           (if (and (:dict col-info) (= :string (:dict-type col-info)))
              ;; Look up string in dict
             (let [dict ^"[Ljava.lang.String;" (:dict col-info)
                   target (nth pred 2)
                   code (loop [i 0]
                          (when (< i (alength dict))
                            (if (.equals ^String (aget dict i) ^String target)
                              (long i)
                              (recur (inc i)))))]
               (if code
                  ;; Found in dict — replace string with dict code
                 (conj out [col-key op code])
                  ;; Not in dict — dict codes are 0..n-1, so -1 never matches
                 (case op
                   :eq  (conj out [col-key :eq -1])  ;; impossible: no row has code -1
                   :neq out  ;; always true: all rows ≠ absent value, drop pred
                   (conj out pred))))
              ;; Not dict-encoded — pass through
             (conj out pred)))
         (conj out pred))))
   [] preds))

;; ============================================================================
;; Strategy Selection
;; ============================================================================

(defn count-pred-types
  "Count long and double predicates given normalized preds and columns."
  [preds columns]
  (let [groups (group-by #(:type (get columns (first %))) preds)]
    {:n-long (count (get groups :int64 []))
     :n-dbl  (count (get groups :float64 []))}))

(def simd-agg-ops
  "Aggregate ops that the SIMD path can handle."
  #{:sum :sum-product :count :count-non-null :min :max :avg})

(defn simd-preds-ok?
  "Check if predicates can work with SIMD agg paths. Allows non-SIMD preds
   (OR/IN/NOT) that get mask-compiled — the mask adds 1 long pred slot."
  [preds columns length]
  (let [{simd-preds true non-simd-preds false}
        (group-by #(simd-pred? % columns) preds)
        mask-slots (if (seq non-simd-preds) 1 0)
        {:keys [n-long n-dbl]} (count-pred-types (vec (or simd-preds [])) columns)]
    (and (<= (+ n-long mask-slots) 4) (<= n-dbl 4)
         (>= (or length 0) 1000))))

(defn simd-eligible?
  "Check if query can use the fused SIMD path (single agg)."
  [preds aggs columns length]
  (and (simd-preds-ok? preds columns length)
       (or (nil? aggs)
           (empty? aggs)
           (and (= 1 (count aggs))
                (simd-agg-ops (:op (first aggs)))))))

(defn multi-agg-simd-eligible?
  "Check if multiple aggs can each be routed through SIMD individually.
   Each SIMD call is ~1ms, so N calls totals ~Nms vs ~250ms scalar."
  [preds aggs columns length]
  (and (> (count aggs) 1)
       (simd-preds-ok? preds columns length)
       (every? #(and (simd-agg-ops (:op %))
                     (nil? (:expr %)))
               aggs)))
