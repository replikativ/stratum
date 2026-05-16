(ns stratum.sql.rewrite
  "Pre-parse SQL rewriter for non-standard syntax that JSqlParser doesn't
   recognize natively.

   Currently handles only ASOF JOIN (DuckDB form): the rewriter strips the
   ASOF keyword and returns a side-channel list of markers indicating which
   top-level joins were originally ASOF. The downstream translator consults
   this list by ordinal position to decide whether to build an ASOF join spec.

   The walker tracks paren depth so JOINs inside subqueries and CTEs don't
   shift the top-level marker indices. UNION/INTERSECT/EXCEPT and ASOF inside
   subqueries are not handled — if a real consumer needs either, the rewriter
   can be extended.

   Modeled on pg-datahike's datahike.pg.rewrite — same shape (token-driven
   walker, set of rules, returns rewritten SQL plus metadata) so the two can
   converge into a shared library when a third project requires it.")

(set! *warn-on-reflection* true)

;; Forward declaration: preprocess-sql calls this, but the implementation
;; (and its helpers parse-temporal-literal / find-balanced-end) live further
;; down to keep the ASOF preprocessor near the top of the file.
(declare preprocess-for-portion-of-valid-time)

;; ============================================================================
;; Low-level scanner — skips strings, comments, identifiers correctly
;; ============================================================================

(defn- skip-line-comment
  "Position at '--'; advance past end-of-line. Returns next index."
  ^long [^String s ^long i]
  (let [n (.length s)]
    (loop [j (+ i 2)]
      (cond
        (>= j n) j
        (= \newline (.charAt s j)) (inc j)
        :else (recur (inc j))))))

(defn- skip-block-comment
  "Position at '/*'; advance past closing '*/'. Returns next index."
  ^long [^String s ^long i]
  (let [n (.length s)]
    (loop [j (+ i 2)]
      (cond
        (>= j n) j
        (and (< (inc j) n)
             (= \* (.charAt s j))
             (= \/ (.charAt s (inc j))))
        (+ j 2)
        :else (recur (inc j))))))

(defn- skip-string
  "Position at a quote char; advance past closing quote, handling doubled-quote
   escape (`''` inside `'...'`, `\"\"` inside `\"...\"`)."
  ^long [^String s ^long i ^Character q]
  (let [n (.length s)]
    (loop [j (inc i)]
      (cond
        (>= j n) j
        (= q (.charAt s j))
        (if (and (< (inc j) n) (= q (.charAt s (inc j))))
          (recur (+ j 2))
          (inc j))
        :else (recur (inc j))))))

(defn- skip-ws-and-comments
  "Advance past whitespace, line comments, and block comments. Returns next index."
  ^long [^String s ^long start]
  (let [n (.length s)]
    (loop [j start]
      (if (>= j n) j
          (let [c (.charAt s j)]
            (cond
              (Character/isWhitespace c) (recur (inc j))
              (and (= \- c) (< (inc j) n) (= \- (.charAt s (inc j))))
              (recur (skip-line-comment s j))
              (and (= \/ c) (< (inc j) n) (= \* (.charAt s (inc j))))
              (recur (skip-block-comment s j))
              :else j))))))

(defn- word-char? [^Character c]
  (or (Character/isLetterOrDigit c) (= \_ c)))

(defn- read-word
  "Read an unquoted identifier or keyword starting at i. Returns [end-index
   lowercase-word] or [i nil] if the char at i isn't a word start."
  [^String s ^long i]
  (let [n (.length s)
        c (when (< i n) (.charAt s i))]
    (if (or (nil? c) (not (or (Character/isLetter ^Character c) (= \_ c))))
      [i nil]
      (loop [j (inc i)]
        (if (and (< j n) (word-char? (.charAt s j)))
          (recur (inc j))
          [j (.toLowerCase (subs s i j))])))))

(defn- next-keyword
  "Skip whitespace+comments then read the next word (lowercase).
   Returns [end-index word-or-nil]."
  [^String s ^long start]
  (let [j (skip-ws-and-comments s start)]
    (read-word s j)))

;; ============================================================================
;; Public: ASOF JOIN preprocessor
;; ============================================================================

(def ^:private join-modifiers
  "Words that can appear between ASOF and JOIN."
  #{"left" "inner" "right" "full" "outer"})

(defn preprocess-sql
  "Strip 'ASOF' before 'JOIN' (or 'ASOF LEFT JOIN' / 'ASOF INNER JOIN').
   Also strip `FOR PORTION OF VALID_TIME FROM x TO y` from DML statements.

   Returns {:sql rewritten-sql
            :asof-markers [marker-or-nil ...]
            :period {:axis :valid_time :from <micros> :to <micros>} | nil}.

   The :asof-markers vector is indexed by top-level join ordinal (0-based);
   each entry is :asof, :asof-left, or nil for non-ASOF joins. The downstream
   translator looks up the marker for each Join in JSqlParser's getJoins()
   list by position.

   :period is attached when the SQL carries a SQL:2011 `FOR PORTION OF
   VALID_TIME` clause; the downstream DML translator picks it up and
   passes it as a temporal slice to the stratum dataset's retract! /
   upsert! primitives.

   Joins inside subqueries (paren-depth > 0) are not counted at the top level
   and are not eligible for ASOF rewriting in this pass."
  [^String sql]
  (let [{:keys [sql period]} (preprocess-for-portion-of-valid-time sql)
        ^String sql sql
        n (.length sql)
        sb (StringBuilder. n)
        markers (java.util.ArrayList.)]
    (loop [i (long 0) paren-depth (long 0)]
      (if (>= i n)
        {:sql (.toString sb)
         :asof-markers (vec markers)
         :period period}
        (let [c (.charAt sql i)]
          (cond
            ;; String literal — copy unchanged
            (= \' c)
            (let [end (long (skip-string sql i \'))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Quoted identifier — copy unchanged
            (= \" c)
            (let [end (long (skip-string sql i \"))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Line comment
            (and (= \- c) (< (inc i) n) (= \- (.charAt sql (inc i))))
            (let [end (long (skip-line-comment sql i))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Block comment
            (and (= \/ c) (< (inc i) n) (= \* (.charAt sql (inc i))))
            (let [end (long (skip-block-comment sql i))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Paren tracking
            (= \( c)
            (do (.append sb c) (recur (inc i) (inc paren-depth)))
            (= \) c)
            (do (.append sb c) (recur (inc i) (dec paren-depth)))

            ;; Word / keyword
            (or (Character/isLetter c) (= \_ c))
            (let [[wend* word] (read-word sql i)
                  wend (long wend*)]
              (cond
                ;; "ASOF" only at depth 0 followed by JOIN (or modifier+JOIN)
                (and (zero? paren-depth) (= "asof" word))
                (let [[k1end* k1] (next-keyword sql wend)
                      k1end (long k1end*)
                      [k2end* k2] (when (contains? join-modifiers k1)
                                    (next-keyword sql k1end))
                      k2end (long (or k2end* k1end))]
                  (cond
                    (= "join" k1)
                    (do (.add markers :asof)
                        ;; Strip "ASOF", emit " JOIN" (preserves original case)
                        (.append sb (subs sql wend k1end))
                        (recur k1end paren-depth))

                    (and (= "left" k1) (= "join" k2))
                    (do (.add markers :asof-left)
                        (.append sb (subs sql wend k2end))
                        (recur k2end paren-depth))

                    (and (= "inner" k1) (= "join" k2))
                    (do (.add markers :asof)
                        (.append sb (subs sql wend k2end))
                        (recur k2end paren-depth))

                    :else
                    ;; ASOF not followed by a recognized JOIN form — pass through
                    (do (.append sb (subs sql i wend))
                        (recur wend paren-depth))))

                ;; "JOIN" at depth 0 with no preceding ASOF: record nil marker
                (and (zero? paren-depth) (= "join" word))
                (do (.add markers nil)
                    (.append sb (subs sql i wend))
                    (recur wend paren-depth))

                :else
                (do (.append sb (subs sql i wend))
                    (recur wend paren-depth))))

            :else
            (do (.append sb c) (recur (inc i) paren-depth))))))))

;; ============================================================================
;; Public: SQL:2011 FOR PORTION OF VALID_TIME preprocessor
;; ============================================================================

(defn- parse-temporal-literal
  "Parse a stratum-temporal *constant-foldable expression* into a
   `long` in `:micros`. Accepts (case-insensitive):

     'YYYY-MM-DD'                — start-of-day UTC
     DATE 'YYYY-MM-DD'           — same
     TIMESTAMP 'YYYY-MM-DD...'   — full ISO instant
     <number>                    — passthrough (already in :micros)
     CURRENT_TIMESTAMP / NOW / NOW()  — System/currentTimeMillis × 1000
     END_OF_TIME / MAX_VALUE     — `Long/MAX_VALUE`
     START_OF_TIME / MIN_VALUE   — `Long/MIN_VALUE`

   Per-row column references and arbitrary expressions are not
   supported here — the preprocessor needs a single scalar at
   parse time to attach to the DDL `:period`. Column-ref periods
   (`FOR PORTION OF VALID_TIME FROM contract_start TO contract_end`)
   would require per-row period evaluation, deferred to a future
   commit. For now, callers that need a column-derived period must
   compute it in application code and pass a literal.

   Throws ex-info on unparseable input."
  [^String s]
  (let [trimmed (.trim s)]
    (cond
      (re-matches #"-?\d+" trimmed) (Long/parseLong trimmed)

      (re-matches #"(?i)CURRENT_TIMESTAMP|NOW(?:\s*\(\s*\))?" trimmed)
      (* 1000 (System/currentTimeMillis))

      (re-matches #"(?i)END_OF_TIME|MAX_VALUE" trimmed)
      Long/MAX_VALUE

      (re-matches #"(?i)START_OF_TIME|MIN_VALUE" trimmed)
      Long/MIN_VALUE

      :else
      (let [body (or (second (re-find #"(?is)^(?:DATE|TIMESTAMP)?\s*'([^']+)'$" trimmed))
                     (throw (ex-info (str "Unparseable temporal literal in FOR PORTION OF "
                                          "VALID_TIME — only string literals (`'YYYY-MM-DD'`, "
                                          "`DATE`/`TIMESTAMP` prefixes), numeric micros, and "
                                          "the constants CURRENT_TIMESTAMP / NOW / "
                                          "END_OF_TIME / START_OF_TIME are recognized. Column "
                                          "references and per-row expressions are not yet "
                                          "supported.")
                                     {:input s})))]
        (cond
          (re-matches #"\d{4}-\d{2}-\d{2}" body)
          (-> (java.time.LocalDate/parse body)
              (.atStartOfDay java.time.ZoneOffset/UTC)
              .toInstant
              .toEpochMilli
              (* 1000))

          :else
          (-> (java.time.Instant/parse body)
              .toEpochMilli
              (* 1000)))))))

(defn- find-balanced-end
  "Starting at position `i` just past `keyword`, scan forward for the next
   keyword from `terminators` at paren-depth 0. Returns the index of the
   terminator. Used to find where the expr after FROM or TO ends."
  ^long [^String sql ^long i terminators]
  (let [n (.length sql)]
    (loop [j i, paren 0]
      (cond
        (>= j n) j

        (= \' (.charAt sql j))
        (recur (skip-string sql j \') paren)

        (= \" (.charAt sql j))
        (recur (skip-string sql j \") paren)

        (= \( (.charAt sql j)) (recur (inc j) (inc paren))
        (= \) (.charAt sql j)) (recur (inc j) (dec paren))

        (and (zero? paren)
             (or (Character/isLetter (.charAt sql j)) (= \_ (.charAt sql j))))
        (let [[wend* w] (read-word sql j)]
          (if (and (zero? paren) (contains? terminators w))
            j
            (recur (long wend*) paren)))

        :else (recur (inc j) paren)))))

(def ^:private dml-tail-keywords
  "Keywords that terminate a `FOR PORTION OF VALID_TIME` clause when
   walking forward through the SQL. Matches the keyword set XTDB v2
   uses in its `Sql.g4:828-829` grammar and adds the SQL DML tail
   tokens we want to preserve (WHERE, SET, VALUES, RETURNING, etc.).
   `to` is included so the FROM scan stops at the optional TO keyword."
  #{"to" "where" "set" "values" "returning" "from" "using" "select"})

(def ^:private dml-tail-keywords-no-to
  "Same as `dml-tail-keywords` but without `to` — used for the TO
   expression's terminator scan."
  #{"where" "set" "values" "returning" "from" "using" "select"})

(defn preprocess-for-portion-of-valid-time
  "Strip SQL:2011 `FOR PORTION OF VALID_TIME FROM <x> [TO <y>]` from a
   DML statement. Returns `{:sql rewritten :period {:axis :from :to}}`
   if the clause is present, otherwise `{:sql sql :period nil}`. The
   downstream parser then sees a plain INSERT / UPDATE / DELETE and the
   server attaches `:period` back when lowering to stratum primitives.

   The `TO` clause is optional — when absent, the period extends to
   `Long/MAX_VALUE` (XTDB v2 calls this `xtdb/end-of-time`,
   `Sql.g4:828-829`). The open-ended form is useful for retracting
   from a point forward.

   Only one occurrence is recognized per statement (DML applies to a
   single target). Recognizes `VALID_TIME` and `SYSTEM_TIME` axes;
   currently only `VALID_TIME` is honored by the server."
  [^String sql]
  ;; `FOR ALL VALID_TIME` / `FOR VALID_TIME ALL` (and the same for
  ;; SYSTEM_TIME) — XTDB v2 grammar `Sql.g4:830`. Strip first and
  ;; emit `:period {:axis ... :from MIN :to MAX}` so the lowering
  ;; treats it as a window spanning all time.
  (if-let [all-m (re-find #"(?is)\bFOR\s+(?:ALL\s+(VALID_TIME|SYSTEM_TIME)|(VALID_TIME|SYSTEM_TIME)\s+ALL)\b" sql)]
    (let [axis (or (nth all-m 1) (nth all-m 2))
          full ^String (first all-m)
          start (.indexOf sql full)
          rewritten (str (subs sql 0 start) (subs sql (+ start (.length full))))]
      {:sql rewritten
       :period {:axis (keyword (.toLowerCase ^String axis))
                :from Long/MIN_VALUE
                :to   Long/MAX_VALUE}})
    (let [m (re-find #"(?is)\bFOR\s+PORTION\s+OF\s+(VALID_TIME|SYSTEM_TIME)\s+FROM\s+" sql)]
      (if-not m
      {:sql sql :period nil}
      (let [[full-prefix axis] m
            start (.indexOf sql ^String full-prefix)
            after-from (+ start (.length ^String full-prefix))
            ;; Walk forward to either a literal TO keyword or to the
            ;; next DML tail keyword (WHERE/SET/...) — that's where
            ;; the FROM expression ends.
            from-end-pos (long (find-balanced-end sql after-from dml-tail-keywords))
            from-expr (subs sql after-from from-end-pos)
            ;; Peek: is the terminator a TO keyword (open-bounded
            ;; form) or a tail keyword (open-ended form)?
            [_ first-kw] (read-word sql (long (skip-ws-and-comments sql from-end-pos)))
            has-to? (= "to" first-kw)
            [tail-pos to-expr]
            (if has-to?
              (let [after-to-kw (long (->> (skip-ws-and-comments sql from-end-pos)
                                           (read-word sql)
                                           first))
                    tp (long (find-balanced-end sql after-to-kw dml-tail-keywords-no-to))]
                [tp (subs sql after-to-kw tp)])
              [from-end-pos nil])
            vf-val (parse-temporal-literal from-expr)
            vt-val (if has-to?
                     (parse-temporal-literal to-expr)
                     Long/MAX_VALUE)
            rewritten (str (subs sql 0 start) (subs sql tail-pos))]
        {:sql rewritten
         :period {:axis (keyword (.toLowerCase ^String axis))
                  :from vf-val
                  :to vt-val}})))))
