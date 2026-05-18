(ns stratum.sql.enum-ddl
  "Hand-rolled parser for `CREATE TYPE name AS ENUM ('v1', 'v2', ...)`.

   JSqlParser 5.2 has no `CreateType` node, so statements of this shape
   never reach the main parsing path — we detect them upstream of
   `CCJSqlParserUtil/parse` and produce the DDL descriptor here.

   Shape returned on success:

     {:op        :create-type-enum
      :type-name <unqualified-name string>
      :values    [<label> ...]   ; declaration order, deduped
      :or-replace? <bool>}

   Modeled on pg-datahike's `datahike.pg.sql.types/parse-create-type-enum`,
   which solved the same problem against the same JSqlParser limitation.
   Schema qualification (e.g. `public.colors`) is accepted and stripped —
   Stratum's registry is server-scoped, not schema-scoped."
  (:require [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Detection

(def ^:private create-type-enum-regex
  ;; Loose prefix recognizer. Whitespace-tolerant, case-insensitive.
  ;; Optional OR REPLACE; optional schema-qualified type name.
  #"(?is)^\s*create\s+(?:or\s+replace\s+)?type\s+\S+(?:\s*\.\s*\S+)?\s+as\s+enum\s*\(")

(defn create-type-enum?
  "True iff `sql` syntactically begins with `CREATE [OR REPLACE] TYPE ... AS ENUM (`.
   Caller is expected to fall through to `parse-create-type-enum` for the
   detailed parse — this is the dispatch gate, not the validation."
  [^String sql]
  (boolean (re-find create-type-enum-regex sql)))

;; ---------------------------------------------------------------------------
;; Tokenizer

(defn- read-string-literal
  "Read a single-quoted SQL string literal starting at index `i` (which
   must point at the opening quote). Returns `[value next-index]`.
   Supports doubled `''` as an embedded single quote."
  [^String sql ^long i]
  (let [n (.length sql)
        sb (StringBuilder.)]
    (loop [j (inc i)]
      (when (>= j n)
        (throw (ex-info "Unterminated string literal in CREATE TYPE"
                        {:error :syntax-error :position i})))
      (let [c (.charAt sql j)]
        (cond
          (not= c \')
          (do (.append sb c) (recur (inc j)))

          ;; Doubled '' → literal '
          (and (< (inc j) n) (= \' (.charAt sql (inc j))))
          (do (.append sb \') (recur (+ j 2)))

          :else
          [(.toString sb) (inc j)])))))

(defn- read-word
  "Read an identifier-like word [A-Za-z0-9_]+ starting at index `i`.
   Returns `[word next-index]`."
  [^String sql ^long i]
  (let [n (.length sql)
        sb (StringBuilder.)]
    (loop [j i]
      (if (and (< j n)
               (let [c (.charAt sql j)]
                 (or (Character/isLetterOrDigit c) (= c \_))))
        (do (.append sb (.charAt sql j)) (recur (inc j)))
        [(.toString sb) j]))))

(defn- tokenize
  "Lex CREATE TYPE syntax into a vector of [kind value] tokens.
   Kinds: :word (string), :string (string), :lparen, :rparen, :comma, :dot, :eof."
  [^String sql]
  (let [n (.length sql)]
    (loop [i 0, acc (transient [])]
      (cond
        (>= i n) (persistent! (conj! acc [:eof nil]))

        (Character/isWhitespace (.charAt sql i))
        (recur (inc i) acc)

        :else
        (let [c (.charAt sql i)]
          (case c
            \( (recur (inc i) (conj! acc [:lparen nil]))
            \) (recur (inc i) (conj! acc [:rparen nil]))
            \, (recur (inc i) (conj! acc [:comma nil]))
            \. (recur (inc i) (conj! acc [:dot nil]))
            \; (recur (inc i) acc)            ; trailing semicolon
            \'
            (let [[lit j] (read-string-literal sql i)]
              (recur j (conj! acc [:string lit])))
            (cond
              (or (Character/isLetter c) (= c \_))
              (let [[w j] (read-word sql i)]
                (recur j (conj! acc [:word w])))
              :else
              (throw (ex-info (str "Unexpected character '" c "' in CREATE TYPE")
                              {:error :syntax-error :position i :char c})))))))))

;; ---------------------------------------------------------------------------
;; Parser helpers

(defn- expect-kind [[[kind value] & rest-toks :as _toks] expected msg]
  (when-not (= kind expected)
    (throw (ex-info msg {:error :syntax-error :expected expected :got kind})))
  [value rest-toks])

(defn- expect-kw [toks kw msg]
  (let [[v rest-toks] (expect-kind toks :word msg)]
    (when-not (= (str/lower-case v) (str/lower-case kw))
      (throw (ex-info msg {:error :syntax-error :expected kw :got v})))
    rest-toks))

(defn- consume-type-name
  "Consume `name` or `schema.name`. Returns [unqualified-name remaining-toks].
   The schema prefix is dropped: Stratum's enum registry is server-scoped."
  [toks]
  (let [[w1 toks] (expect-kind toks :word "expected type name after CREATE TYPE")]
    (if (= :dot (first (first toks)))
      (let [[w2 toks] (expect-kind (rest toks) :word "expected identifier after `.`")]
        [w2 toks])
      [w1 toks])))

(defn- consume-values
  "Consume a comma-separated, paren-delimited list of string literals.
   Returns [values remaining-toks]. Empty list rejected."
  [toks]
  (let [[_ toks] (expect-kind toks :lparen "expected `(` after AS ENUM")]
    (loop [toks toks, acc []]
      (let [[k v] (first toks)]
        (cond
          (= k :string)
          (let [acc (conj acc v)
                next-toks (rest toks)
                [k2 _] (first next-toks)]
            (cond
              (= k2 :comma) (recur (rest next-toks) acc)
              (= k2 :rparen) [acc (rest next-toks)]
              :else (throw (ex-info "expected `,` or `)` in ENUM value list"
                                    {:error :syntax-error :got k2}))))

          (= k :rparen)
          (if (zero? (count acc))
            (throw (ex-info "ENUM must declare at least one value"
                            {:error :syntax-error}))
            [acc (rest toks)])

          :else
          (throw (ex-info "expected string literal in ENUM value list"
                          {:error :syntax-error :got k})))))))

;; ---------------------------------------------------------------------------
;; Public entry

(defn parse-create-type-enum
  "Parse a `CREATE [OR REPLACE] TYPE [schema.]name AS ENUM (...)` statement.
   Returns the DDL descriptor on success, or throws ex-info with
   `:error :syntax-error` on malformed input."
  [^String sql]
  (let [toks   (tokenize sql)
        toks   (expect-kw toks "create" "expected CREATE")
        ;; Detect optional OR REPLACE
        [or-replace? toks]
        (let [[k v] (first toks)]
          (if (and (= k :word) (= (str/lower-case v) "or"))
            [true (-> toks rest (expect-kw "replace" "expected REPLACE after OR"))]
            [false toks]))
        toks   (expect-kw toks "type" "expected TYPE")
        [name toks] (consume-type-name toks)
        toks   (expect-kw toks "as" "expected AS")
        toks   (expect-kw toks "enum" "expected ENUM")
        [values toks] (consume-values toks)
        ;; Detect trailing junk (only :eof allowed after the closing paren).
        [trailing-kind] (first toks)]
    (when-not (= trailing-kind :eof)
      (throw (ex-info "unexpected tokens after ENUM value list"
                      {:error :syntax-error :got trailing-kind})))
    ;; Duplicate values are rejected — both PG and DuckDB do this.
    (let [seen-dupes (->> values frequencies (filter #(> (val %) 1)) (map key))]
      (when (seq seen-dupes)
        (throw (ex-info (str "ENUM values must be unique; duplicates: "
                             (str/join ", " seen-dupes))
                        {:error :syntax-error :duplicates seen-dupes}))))
    {:op          :create-type-enum
     :type-name   name
     :values      values
     :or-replace? or-replace?}))
