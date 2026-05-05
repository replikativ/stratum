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

   Returns {:sql rewritten-sql :asof-markers [marker-or-nil ...]}.

   The :asof-markers vector is indexed by top-level join ordinal (0-based);
   each entry is :asof, :asof-left, or nil for non-ASOF joins. The downstream
   translator looks up the marker for each Join in JSqlParser's getJoins()
   list by position.

   Joins inside subqueries (paren-depth > 0) are not counted at the top level
   and are not eligible for ASOF rewriting in this pass."
  [^String sql]
  (let [n (.length sql)
        sb (StringBuilder. n)
        markers (java.util.ArrayList.)]
    (loop [i (long 0) paren-depth (long 0)]
      (if (>= i n)
        {:sql (.toString sb)
         :asof-markers (vec markers)}
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
