# Query Engine

The query engine (`stratum.query`) compiles declarative query maps into optimized execution plans that dispatch to Java SIMD operations. It handles column preparation, expression evaluation, predicate compilation, aggregation dispatch, group-by routing, join execution, and result decoding.

## Query DSL

Queries are maps with the following optional keys:

```clojure
{:from    {col-kw data ...}                ;; Column data (required)
 :join    [{:with {col data} :on pred :type :inner}]  ;; Hash joins
 :where   [pred ...]                       ;; Filter predicates
 :select  [col-or-expr ...]               ;; Projection (with :as aliases)
 :agg     [agg-spec ...]                  ;; Aggregations
 :group   [col-kw ...]                    ;; Group-by columns
 :having  [pred ...]                      ;; Post-aggregation filter
 :order   [[col dir] ...]                 ;; Sort (:asc/:desc)
 :limit   n                               ;; Result limit
 :offset  n                               ;; Result offset
 :distinct true                           ;; SELECT DISTINCT
 :result  :columns}                       ;; Columnar output format
```

### Predicate Syntax

Both keyword vectors and s-expressions are supported:

```clojure
[:< :col 100]           ;; keyword form
'(< :col 100)           ;; s-expression form (Datahike-compatible)
[:between :col 10 20]   ;; range: 10 <= col < 20
[:like :name "%foo%"]   ;; string pattern (case-sensitive)
[:ilike :name "%foo%"]  ;; string pattern (case-insensitive)
[:in :status [1 2 3]]   ;; set membership
[:or [:< :a 5] [:> :b 10]]  ;; disjunction
[:is-null :col]         ;; NULL check
[:not [:= :col 0]]      ;; negation
```

### Aggregation Specs

```clojure
[:sum :col]                    ;; SUM(col)
[:avg :col]                    ;; AVG(col)
[:count]                       ;; COUNT(*)
[:count-distinct :col]         ;; COUNT(DISTINCT col)
[:min :col]  [:max :col]       ;; MIN/MAX
[:stddev :col]                 ;; STDDEV(col)
[:variance :col]               ;; VARIANCE(col)
[:corr :col1 :col2]           ;; CORR(col1, col2)
[:sum [:* :price :qty]]        ;; SUM(price * qty) — expression in agg
[:sum-product :price :qty]     ;; SUM(price * qty) — direct
[:median :col]                 ;; MEDIAN(col)
[:percentile :col 0.95]        ;; PERCENTILE_CONT(0.95, col)
[:approx-quantile :col 0.95]   ;; Approximate quantile via t-digest
```

### Expression Syntax

```clojure
[:+ :a :b]                     ;; Arithmetic: + - * / % **
[:abs :col]                    ;; Math: abs sqrt log log10 exp round floor ceil sign
[:date-trunc :day :ts]         ;; Date truncation: year month day hour minute
[:extract :hour :ts]           ;; Date extraction: year month day hour minute second
[:date-add :days 30 :ts]       ;; Date arithmetic
[:coalesce :col 0]             ;; NULL handling
[:nullif :col 0]               ;; Returns NULL if col = value
[:greatest :a :b :c]           ;; Maximum of multiple values
[:least :a :b :c]              ;; Minimum of multiple values
[:upper :name]                 ;; String: upper lower substr replace trim concat length
[:cast :col :double]           ;; Type cast
```

## Column Scoping and Composition

### Flat Namespace

The DSL operates over a single flat namespace. Every clause (`:where`, `:agg`, `:group`, `:select`, `:having`, `:order`) resolves column names by keyword lookup against one merged column map. There is no scope nesting, no table qualification, and no quoting — just keywords:

```clojure
{:from  {:price (double-array [10 20 30])
         :qty   (long-array [1 2 3])}
 :where [[:> :qty 1]]          ;; :qty resolved from :from
 :agg   [[:sum [:* :price :qty]]]}  ;; :price and :qty both visible
```

### Join Merge

`:join` merges the dimension table's columns into the same flat namespace. After the join, all `:from` columns and all `:with` columns are visible to downstream clauses:

```clojure
{:from  {:order-id (long-array [1 2 3])
         :cust-id  (long-array [10 20 10])}
 :join  [{:with {:cust-id  (long-array [10 20])   ;; key column
                 :region   (into-array String ["US" "EU"])}  ;; payload
          :on   [:= :cust-id :cust-id]   ;; left.cust-id = right.cust-id
          :type :inner}]
 :group [:region]             ;; :region comes from :with
 :agg   [[:count]]}           ;; both sides visible
```

**Key collision**: when a column name appears in both `:from` and `:with`, the right (`:with`) side wins. The join key column from the right side is dropped after the join (it's redundant — it equals the left-side key by definition).

If you need both sides of a collision, rename the column in one of the input maps before building the query:

```clojure
(let [orders  {:cust-id orders-cust-id :amount orders-amount}
      renamed {:cust-key dim-cust-id :tier dim-tier}]  ;; renamed to avoid collision
  (st/q {:from orders
         :join [{:with renamed :on [:= :cust-id :cust-key] :type :left}]
         :group [:tier]
         :agg [[:sum :amount]]}))
```

### Composition via Clojure (DSL equivalent of CTEs/subqueries)

SQL CTEs and subqueries name intermediate results:

```sql
WITH monthly AS (
  SELECT DATE_TRUNC('month', ts) AS month, SUM(amount) AS total
  FROM orders GROUP BY 1
)
SELECT month, total FROM monthly WHERE total > 1000
```

In the DSL, intermediate results are Clojure values. Use `def` or `let` to bind them:

```clojure
(let [monthly (st/q {:from    orders
                     :group   [[:date-trunc :month :ts]]
                     :agg     [[:sum :amount]]})]
  ;; monthly is a sequence of maps: [{:date-trunc 1704067200 :sum 5000} ...]
  ;; Extract to column arrays for the next stage:
  (st/q {:from   {:month (long-array (map :date-trunc monthly))
                  :total (double-array (map :sum monthly))}
         :where  [[:> :total 1000]]
         :select [:month :total]}))
```

When the SQL layer translates a query with CTEs or subqueries, it does exactly this: it executes each CTE eagerly, collects the results into column arrays, and wires them as `:from` data in the outer query map. No lazy references, no symbolic names at the DSL level.

### SQL → DSL Mapping

The SQL layer (`stratum.sql`) translates SQL strings to query maps. Subquery results are materialized before the outer query runs:

```sql
-- SQL
SELECT region, SUM(total) FROM
  (SELECT region, SUM(amount) AS total FROM orders GROUP BY region)
WHERE total > 500 GROUP BY region
```

```clojure
;; What the SQL layer produces (schematically):
(let [inner-result (st/q {:from orders :group [:region] :agg [[:sum :amount]]})]
  (st/q {:from  {:region (into-array String (map :region inner-result))
                 :total  (double-array (map :sum inner-result))}
         :where [[:> :total 500]]
         :group [:region]
         :agg   [[:sum :total]]}))
```

This means the DSL is not less expressive than SQL — it has the same compositional power, expressed through Clojure's own value model rather than SQL's naming syntax.

## Execution Pipeline

The `q` function processes a query through these stages:

1. **prepare-columns**: Resolve `:from` data. Detect column types (long[], double[], dict-encoded). Build column lookup map.

2. **Join execution**: If `:join` is present, build hash tables on dimension keys, probe with fact keys, gather matched rows. Fused join+group-by attempted first for eligible queries.

3. **Expression pre-computation**: Evaluate expressions in `:where`, `:agg`, `:group`, `:select` into materialized arrays using `eval-expr-vectorized` (Java array ops).

4. **Zone map pruning** (index inputs): Classify chunks as skip/stats-only/SIMD using ChunkStats. Materialize only surviving chunks via `idx-materialize-to-array-pruned`.

5. **Predicate separation**: Split predicates into long-typed and double-typed groups. Non-SIMD predicates (OR, IN, NOT-IN) compiled to bytecode masks.

6. **Dispatch**: Route to optimal execution strategy (see below).

7. **Result decode**: Convert Java arrays to Clojure maps (or columnar format if `:result :columns`).

8. **Post-processing**: Apply `:having`, `:order`, `:limit`, `:offset`, `:distinct`.

## Dispatch Decision Tree

The engine selects the fastest execution strategy based on query shape:

```
Is this a projection-only query (no agg, no group)?
  → Yes: materialize columns, apply select/where/order/limit
  → No: continue

Is this a fused extract+count (EXTRACT expr + all-COUNT aggs)?
  → Yes: fusedExtractCountDenseParallel

Is the input a PersistentColumnIndex eligible for chunked group-by?
  → Yes: execute-chunked-group-by (streams over chunks, no materialization)

Is this a fused join+group-by (single INNER join, keyword group cols, no WHERE)?
  → Yes: fusedJoinGroupAggregateDenseRange

Is this a single agg with ≤4 long + ≤4 double predicates on ≥1000 rows?
  → Yes: Is it COUNT?
    → Yes: fusedSimdCountParallel
    → No: fusedSimdParallel (SUM/SUM_PRODUCT/MIN/MAX)

Are all aggs SUM/SUM_PRODUCT/COUNT/AVG with ≤4 aggs?
  → Yes: Are all agg columns long[]?
    → Yes: fusedSimdMultiSumAllLongParallel (LongVector accumulators)
    → No: fusedSimdMultiSumParallel

Is this a group-by?
  → Yes: Is max key ≤ dense limit (200K)?
    → Yes: Has VARIANCE/CORR aggs?
      → Yes: fusedFilterGroupAggregateDenseVarParallel
      → No: All COUNT?
        → Yes: fusedFilterGroupCountDenseParallel
        → No: fusedFilterGroupAggregateDenseParallel
    → No (high cardinality):
      → fusedFilterGroupAggregatePartitioned (radix-partitioned hash)

Is this a percentile/median/approx-quantile?
  → Yes: Scalar path — collect matching values, QuickSelect or t-digest

Fallback: N-pass (one pass per agg) or scalar (< 1000 rows)
```

## Statistical Aggregates

Statistical aggregates use specialized algorithms that don't fit the SIMD accumulator pattern:

- **`:median`** — Sugar for `[:percentile :col 0.5]`
- **`:percentile`** — Exact percentile via Hoare's QuickSelect (O(N) average). Copies matching values to work array, partitions in-place. For grouped queries, collects per-group values via ArrayList then sorts.
- **`:approx-quantile`** — Approximate quantile via t-digest (Dunning 2019). Bounded memory (~6.4KB per digest), O(N) insertion. For ungrouped queries, uses single digest. For grouped queries, per-group digests in flat arrays. Accuracy within ±1% for 100K+ rows.

These are dispatched through the scalar aggregation path (`execute-scalar-aggs` and the collection-agg branch of hash group-by). They cannot use the SIMD fused paths because they require collecting individual values rather than accumulating running statistics.

## Dictionary Encoding

String columns are dictionary-encoded for efficient group-by and LIKE operations:

```clojure
(q/encode-column (into-array String ["US" "EU" "US" "JP" "EU"]))
;; => {:codes (long-array [0 1 0 2 1])
;;     :dict  (into-array String ["US" "EU" "JP"])}
```

The codes are sequential integers (0, 1, 2, ...), enabling direct array indexing in dense group-by. Pre-encoding with `q/encode-column` is critical for performance — per-query encoding adds 3-4x overhead.

LIKE patterns on dictionary-encoded columns first filter the dictionary (e.g., which strings contain "foo"), then check per-row codes against a bitset of matching dictionary entries.

## Compiled Predicate Masks

Predicates that can't be expressed as SIMD operations (`:or`, `:in`, `:not-in`) are compiled to JVM bytecode:

```clojure
;; [:or [:< :a 5] [:> :b 10]] compiles to:
(let [mask (long-array n)]
  (dotimes [i n]
    (aset mask i
      (if (or (< (aget a i) 5) (> (aget b i) 10)) 1 0)))
  mask)
```

The compiled mask is passed as `[:__mask :eq 1]` — a SIMD-compatible equality predicate that integrates with the fused filter+aggregate pipeline.

## Expression Evaluation

`eval-expr-vectorized` evaluates expressions to materialized arrays using Java array operations:

```clojure
;; [:* :price :qty] →
(ColumnOps/arrayMul price-arr qty-arr length)

;; [:- :price 100.0] →
(ColumnOps/arraySubScalar 100.0 price-arr length)

;; [:date-trunc :minute :ts] →
(ColumnOps/arrayDateTruncMinute ts-arr length)
```

All operations produce new arrays. Scalar values are handled by dedicated scalar-op methods (e.g., `arraySubScalar`) to avoid allocating broadcast arrays.

`eval-expr-to-long` is used for date expressions in group-by contexts, returning `long[]` directly instead of routing through `double[]` conversion.

## Columnar Output

By default, results are returned as sequences of maps. The `:result :columns` option returns a map of column name → array:

```clojure
;; Default: [{:region "US" :sum 100} {:region "EU" :sum 200}]
;; Columnar: {:region (String[] ...) :sum (double-array ...) :n-rows 2}
```

For high-cardinality group-by (millions of groups), columnar output is 15x faster because it skips per-row map creation.

## EXPLAIN

```clojure
(q/q (assoc q :explain true))
;; => {:strategy :fused-simd-parallel
;;     :n-rows 6000000
;;     :predicates [{:col :shipdate :type :range :bounds [8766 9131]} ...]
;;     :agg {:type :sum-product :col1 :price :col2 :discount}}
```

Returns the execution plan without running the query.

## Related Documentation

- [Architecture](architecture.md) — System overview
- [SIMD Internals](simd-internals.md) — How the Java SIMD paths work
- [Storage and Indices](storage-and-indices.md) — Index-aware query execution
- [SQL Interface](sql-interface.md) — SQL queries translated to this DSL
