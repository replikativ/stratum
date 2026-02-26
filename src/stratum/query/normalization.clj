(ns stratum.query.normalization
  "Normalization of DSL input into canonical internal form.

   All three public functions accept both keyword operators (Stratum-style)
   and symbol operators (Datahike/Datomic-style) so queries written for
   either dialect work without conversion.

   Output forms:
     normalize-pred  → [col-kw op & args]   e.g. [:shipdate :range 8766 9131]
     normalize-expr  → {:op kw :args [...]} or bare keyword/number
     normalize-agg   → {:op kw :col/:cols kw :as alias-or-nil}")

(set! *warn-on-reflection* true)

;; ============================================================================
;; Utilities
;; ============================================================================

(defn strip-ns
  "Strip namespace from keyword. Non-keywords pass through."
  [k]
  (if (and (keyword? k) (namespace k))
    (keyword (name k))
    k))

;; ============================================================================
;; Expression normalization
;; ============================================================================

(defn normalize-expr
  "Normalize an arithmetic expression like [:* :a :b] to internal form.
   Returns {:op :mul :args [...]} or just a column keyword.
   Args are recursively normalized so nested exprs like
   [:* :price [:- 1 :discount]] become trees."
  [expr]
  (cond
    (keyword? expr) (strip-ns expr)
    (number? expr)  expr
    (string? expr)  expr
    :else
    (let [items (vec expr)
          op    (first items)
          kw-op (if (symbol? op) (keyword (name op)) op)]
      ;; CASE/WHEN must be handled before args are normalized
      (if (= :case kw-op)
        {:op :case
         :branches (mapv (fn [branch]
                           (let [b (vec branch)]
                             (if (= :else (first b))
                               {:op :else :val (normalize-expr (second b))}
                               {:pred (first b)
                                :val  (normalize-expr (second b))})))
                         (subvec items 1))}
        (let [args (mapv normalize-expr (subvec items 1))]
          (case kw-op
            (:* :mul)       {:op :mul  :args args}
            (:+ :add)       {:op :add  :args args}
            (:- :sub)       {:op :sub  :args args}
            (:/ :div)       {:op :div  :args args}
            (:abs)          {:op :abs  :args args}
            (:sqrt)         {:op :sqrt :args args}
            (:log :ln)      {:op :log  :args args}
            (:log10)        {:op :log10 :args args}
            (:exp)          {:op :exp  :args args}
            (:round)        {:op :round :args args}
            (:floor)        {:op :floor :args args}
            (:ceil)         {:op :ceil  :args args}
            (:sign :signum) {:op :sign  :args args}
            (:mod :%)       {:op :mod   :args args}
            (:pow)          {:op :pow   :args args}
            (:year)         {:op :year         :args args}
            (:month)        {:op :month        :args args}
            (:day)          {:op :day          :args args}
            (:hour)         {:op :hour         :args args}
            (:minute)       {:op :minute       :args args}
            (:second)       {:op :second       :args args}
            (:day-of-week)  {:op :day-of-week  :args args}
            (:week-of-year) {:op :week-of-year :args args}
            (:date-trunc)    {:op :date-trunc    :args args}
            (:date-add)      {:op :date-add      :args args}
            (:date-diff)     {:op :date-diff     :args args}
            (:epoch-days)    {:op :epoch-days    :args args}
            (:epoch-seconds) {:op :epoch-seconds :args args}
            (:coalesce)  {:op :coalesce  :args args}
            (:nullif)    {:op :nullif    :args args}
            (:greatest)  {:op :greatest  :args args}
            (:least)     {:op :least     :args args}
            (:length)    {:op :length    :args args}
            (:upper)     {:op :upper     :args args}
            (:lower)     {:op :lower     :args args}
            (:substr)    {:op :substr    :args args}
            (:replace)   {:op :replace   :args args}
            (:trim)      {:op :trim      :args args}
            (:concat)    {:op :concat    :args args}
            (:cast)      {:op :cast      :args args}
            (throw (ex-info (str "Unknown expression operator: " op)
                            {:op op :expr expr}))))))))

;; ============================================================================
;; Predicate normalization
;; ============================================================================

(def ^:private symbol->pred-op
  {'<       :lt
   '>       :gt
   '<=      :lte
   '>=      :gte
   '=       :eq
   'not=    :neq
   '!=      :neq
   'between :between})

(def ^:private keyword->pred-op
  {:<       :lt
   :>       :gt
   :<=      :lte
   :>=      :gte
   :=       :eq
   :!=      :neq
   :not=    :neq
   :between :between})

(defn normalize-pred
  "Normalize a predicate to internal form [col-name op & args].

   Accepts both keyword operators (Stratum-style) and symbol operators
   (Datahike/Datomic-style):

     [:< :col 5]                → [:col :lt 5]
     '(< :col 5)                → [:col :lt 5]
     [:between :col 10 20]      → [:col :range 10 20]
     [:> [:* :a :b] 1000]       → [{:op :mul :args [:a :b]} :gt 1000]
     [:not [:< :col 5]]         → [:col :gte 5]
     [:or pred1 pred2 ...]      → [:__or :or pred1 pred2 ...]
     [:in :col 1 2 3]           → [:col :in #{1 2 3}]"
  [pred]
  (let [items  (vec pred)
        items  (if (and (>= (count items) 2) (keyword? (second items)))
                 (assoc items 1 (strip-ns (second items)))
                 items)
        op-raw (first items)]
    (cond
      (or (= :or op-raw) (= 'or op-raw))
      (into [:__or :or] (mapv normalize-pred (subvec items 1)))

      (or (= :not op-raw) (= 'not op-raw))
      (let [inner      (normalize-pred (second items))
            inner-col  (first inner)
            inner-op   (second inner)
            negated-op (case inner-op
                         :lt          :gte
                         :gt          :lte
                         :lte         :gt
                         :gte         :lt
                         :eq          :neq
                         :neq         :eq
                         :range       :not-range
                         :not-range   :range
                         :in          :not-in
                         :not-in      :in
                         :like        :not-like
                         :not-like    :like
                         :ilike       :not-ilike
                         :not-ilike   :ilike
                         :is-null     :is-not-null
                         :is-not-null :is-null
                         (throw (ex-info (str "Cannot negate op: " inner-op) {:pred pred})))]
        (into [inner-col negated-op] (subvec inner 2)))

      (or (= :in op-raw) (= 'in op-raw))
      (let [col      (second items)
            raw-vals (subvec items 2)
            vals     (if (every? number? raw-vals)
                       (set (mapv double raw-vals))
                       (set raw-vals))]
        [col :in vals])

      (or (= :not-in op-raw) (= 'not-in op-raw))
      (let [col      (second items)
            raw-vals (subvec items 2)
            vals     (if (every? number? raw-vals)
                       (set (mapv double raw-vals))
                       (set raw-vals))]
        [col :not-in vals])

      (or (= :is-null op-raw)     (= 'is-null op-raw))     [(second items) :is-null]
      (or (= :is-not-null op-raw) (= 'is-not-null op-raw)) [(second items) :is-not-null]
      (or (= :like op-raw)        (= 'like op-raw))        [(second items) :like     (nth items 2)]
      (or (= :not-like op-raw)    (= 'not-like op-raw))    [(second items) :not-like (nth items 2)]
      (or (= :ilike op-raw)       (= 'ilike op-raw))       [(second items) :ilike    (nth items 2)]
      (or (= :not-ilike op-raw)   (= 'not-ilike op-raw))   [(second items) :not-ilike (nth items 2)]
      (or (= :contains op-raw)    (= 'contains op-raw))    [(second items) :contains   (nth items 2)]
      (or (= :starts-with op-raw) (= 'starts-with op-raw)) [(second items) :starts-with (nth items 2)]
      (or (= :ends-with op-raw)   (= 'ends-with op-raw))   [(second items) :ends-with   (nth items 2)]

      (keyword? op-raw)
      (let [op   (get keyword->pred-op op-raw op-raw)
            col  (second items)
            args (subvec items 2)]
        (if (= op :between)
          (into [(if (sequential? col) (normalize-expr col) col) :range] args)
          (into [(if (sequential? col) (normalize-expr col) col) op] args)))

      (symbol? op-raw)
      (let [op   (get symbol->pred-op op-raw)
            col  (second items)
            args (subvec items 2)]
        (when-not op
          (throw (ex-info (str "Unknown predicate operator: " op-raw)
                          {:op op-raw :pred pred})))
        (if (= op :between)
          (into [(if (sequential? col) (normalize-expr col) col) :range] args)
          (into [(if (sequential? col) (normalize-expr col) col) op] args)))

      :else
      (throw (ex-info (str "Invalid predicate format: " pred) {:pred pred})))))

;; ============================================================================
;; Aggregation normalization
;; ============================================================================

(def ^:private symbol->agg-op
  {'sum            :sum
   'count          :count
   'count-non-null :count-non-null
   'min            :min
   'max            :max
   'avg            :avg
   'sum-product    :sum-product
   'variance       :variance
   'variance-pop   :variance-pop
   'stddev         :stddev
   'stddev-pop     :stddev-pop
   'median          :median
   'percentile      :percentile
   'approx-quantile :approx-quantile
   'count-distinct  :count-distinct})

(def ^:private keyword->agg-op
  {:sum            :sum
   :count          :count
   :count-non-null :count-non-null
   :min            :min
   :max            :max
   :avg            :avg
   :sum-product    :sum-product
   :variance       :variance
   :variance-pop   :variance-pop
   :stddev         :stddev
   :stddev-pop     :stddev-pop
   :median          :median
   :percentile      :percentile
   :approx-quantile :approx-quantile
   :count-distinct  :count-distinct})

(defn normalize-agg
  "Normalize an aggregation spec to internal form.

   Accepts both keyword operators (Stratum-style) and symbol operators
   (Datahike/Datomic-style):

     [:sum :col]                 → {:op :sum :col :col :as nil}
     '(sum :col)                 → {:op :sum :col :col :as nil}
     [:as [:sum :col] :name]     → {:op :sum :col :col :as :name}
     [:count]                    → {:op :count :col nil :as nil}
     [:sum [:* :a :b]]           → {:op :sum-product :cols [:a :b] :as nil}"
  [agg]
  (let [items (vec agg)]
    (if (= :as (first items))
      (let [inner (normalize-agg (second items))
            alias (nth items 2)]
        (assoc inner :as alias))
      (let [op-raw     (first items)
            op         (cond
                         (keyword? op-raw) (get keyword->agg-op op-raw op-raw)
                         (symbol? op-raw)  (get symbol->agg-op op-raw)
                         :else (throw (ex-info (str "Unknown agg op: " op-raw)
                                               {:op op-raw :agg agg})))
            rest-items (subvec items 1)]
        (cond
          (empty? rest-items)
          {:op op :col nil :as nil}

          (and (= :corr op) (= 2 (count rest-items)) (every? keyword? rest-items))
          {:op :corr :cols (mapv strip-ns rest-items) :as nil}

          (and (= :percentile op) (= 2 (count rest-items)) (number? (second rest-items)))
          {:op :percentile :col (strip-ns (first rest-items)) :param (double (second rest-items)) :as nil}

          (and (= :approx-quantile op) (= 2 (count rest-items)) (number? (second rest-items)))
          {:op :approx-quantile :col (strip-ns (first rest-items)) :param (double (second rest-items)) :as nil}

          (and (= 1 (count rest-items))
               (or (sequential? (first rest-items)) (map? (first rest-items))))
          (let [expr               (if (map? (first rest-items))
                                     (first rest-items)
                                     (normalize-expr (first rest-items)))
                is-simple-mul-sum? (and (= :sum op)
                                        (= :mul (:op expr))
                                        (= 2 (count (:args expr)))
                                        (every? keyword? (:args expr)))]
            (if is-simple-mul-sum?
              {:op :sum-product :cols (:args expr) :as nil}
              {:op op :expr expr :as nil}))

          :else
          (let [col (strip-ns (first rest-items))]
            (if (and (= :count op) (some? col))
              {:op :count-non-null :col col :as :count}
              {:op op :col col :as nil})))))))

;; ============================================================================
;; Aggregate alias deduplication
;; ============================================================================

(defn auto-alias-aggs
  "Auto-generate unique aliases for aggregations that would collide.
   When multiple aggs share the same default alias (op name), appends the
   column name to disambiguate: :sum → :sum_price, :sum_quantity.
   Single occurrences keep the short alias for backward compatibility."
  [aggs]
  (let [defaults (mapv (fn [agg] (or (:as agg) (:op agg))) aggs)
        freq     (frequencies defaults)
        dupes    (set (keep (fn [[k v]] (when (> v 1) k)) freq))]
    (if (empty? dupes)
      aggs
      (let [used (atom #{})]
        (mapv (fn [agg]
                (if (and (nil? (:as agg)) (contains? dupes (:op agg)))
                  (let [base-suffix (cond
                                      (:col agg)  (name (:col agg))
                                      (:cols agg) (apply str (interpose "_" (map name (:cols agg))))
                                      (:expr agg) "expr"
                                      :else       "0")
                        op-name     (name (:op agg))
                        candidate   (str op-name "_" base-suffix)
                        suffix      (if (contains? @used candidate)
                                      (loop [n 2]
                                        (let [s (str candidate "_" n)]
                                          (if (contains? @used s) (recur (inc n)) s)))
                                      candidate)]
                    (swap! used conj suffix)
                    (assoc agg :as (keyword suffix)))
                  agg))
              aggs)))))
