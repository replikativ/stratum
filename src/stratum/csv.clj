(ns stratum.csv
  "CSV import for Stratum.

   Reads CSV files into StratumDataset suitable for queries.
   Uses PostgreSQL NULL semantics: unquoted empty fields → NULL,
   quoted empty \"\" → empty string.

   Usage:
     (from-csv \"data/orders.csv\")
     (from-csv \"data/orders.csv\" :separator \\tab :types {\"price\" :double})
     (from-maps [{:name \"Alice\" :age 30} {:name \"Bob\" :age 25}])"
  (:require [clojure.java.io :as io]
            [stratum.dataset :as dataset])
  (:import [java.io PushbackReader Reader StringReader EOFException]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; CSV Reader (forked from clojure.data.csv 1.1.0, EPL-1.0)
;; Modified: unquoted empty → nil (PostgreSQL NULL), quoted empty → ""
;; ============================================================================

(def ^:private ^:const csv-lf (int \newline))
(def ^:private ^:const csv-cr (int \return))
(def ^:private ^:const csv-eof -1)

(defn- read-quoted-cell
  "Read a quoted CSV cell. Returns [sentinel was-quoted=true]."
  [^PushbackReader reader ^StringBuilder sb sep quote]
  (loop [ch (.read reader)]
    (condp == ch
      quote (let [next-ch (.read reader)]
              (condp == next-ch
                quote (do (.append sb (char quote))
                          (recur (.read reader)))
                sep [:sep true]
                csv-lf [:eol true]
                csv-cr (let [next-next-ch (.read reader)]
                         (when (not= next-next-ch csv-lf)
                           (.unread reader next-next-ch))
                         [:eol true])
                csv-eof [:eof true]
                (throw (Exception. ^String (format "CSV error (unexpected character: %c)" next-ch)))))
      csv-eof (throw (EOFException. "CSV error (unexpected end of file)"))
      (do (.append sb (char ch))
          (recur (.read reader))))))

(defn- read-cell
  "Read a CSV cell. Returns [sentinel was-quoted?]."
  [^PushbackReader reader ^StringBuilder sb sep quote]
  (let [first-ch (.read reader)]
    (if (== first-ch quote)
      (read-quoted-cell reader sb sep quote)
      (loop [ch first-ch]
        (condp == ch
          sep [:sep false]
          csv-lf [:eol false]
          csv-cr (let [next-ch (.read reader)]
                   (when (not= next-ch csv-lf)
                     (.unread reader next-ch))
                   [:eol false])
          csv-eof [:eof false]
          (do (.append sb (char ch))
              (recur (.read reader))))))))

(defn- read-record
  "Read one CSV record. Unquoted empty → nil, quoted empty → \"\"."
  [reader sep quote]
  (loop [record (transient [])]
    (let [cell (StringBuilder.)
          [sentinel quoted?] (read-cell reader cell sep quote)
          s (str cell)
          val (if (and (= s "") (not quoted?)) nil s)]
      (if (= sentinel :sep)
        (recur (conj! record val))
        [(persistent! (conj! record val)) sentinel]))))

(defn- read-csv-rows
  "Lazily read CSV rows. Unquoted empty → nil, quoted empty → \"\"."
  [^PushbackReader reader sep quote]
  (lazy-seq
   (let [[record sentinel] (read-record reader sep quote)]
     (case sentinel
       :eol (cons record (read-csv-rows reader sep quote))
       :eof (when-not (= record [nil])
              (cons record nil))))))

(defn read-csv
  "Read CSV with PostgreSQL NULL semantics.
   Unquoted empty → nil (NULL), quoted empty \"\" → empty string.
   Options: :separator (default \\,), :quote (default \\\")."
  [input & {:keys [separator quote] :or {separator \, quote \"}}]
  (let [reader (cond
                 (instance? PushbackReader input) input
                 (instance? Reader input) (PushbackReader. input)
                 :else (PushbackReader. (StringReader. (str input))))]
    (read-csv-rows reader (int separator) (int quote))))

;; ============================================================================
;; Column Building
;; ============================================================================

(defn- try-parse-long [^String s]
  (try (Long/parseLong s)
       (catch NumberFormatException _ nil)))

(defn- try-parse-double [^String s]
  (try (Double/parseDouble s)
       (catch NumberFormatException _ nil)))

(defn- detect-type
  "Detect column type from a sample of non-empty values.
   Tries Long first, then Double, falls back to String."
  [values]
  (let [sample (take 1000 (remove #(or (nil? %) (= "" %)) values))]
    (if (empty? sample)
      :string
      (cond
        (every? #(some? (try-parse-long %)) sample) :long
        (every? #(some? (try-parse-double %)) sample) :double
        :else :string))))

(defn- build-column
  "Build a typed array from string values.
   nil → NULL sentinel (Long/MIN_VALUE, NaN, or null String).
   Empty string → NULL for numerics, preserved for strings."
  [values col-type]
  (let [n (count values)]
    (case col-type
      :long (let [arr (long-array n)]
              (dotimes [i n]
                (let [v (nth values i)]
                  (aset arr i (if (or (nil? v) (= "" v))
                                Long/MIN_VALUE
                                (Long/parseLong ^String v)))))
              arr)
      :double (let [arr (double-array n)]
                (dotimes [i n]
                  (let [v (nth values i)]
                    (aset arr i (if (or (nil? v) (= "" v))
                                  Double/NaN
                                  (Double/parseDouble ^String v)))))
                arr)
      ;; String: nil (unquoted empty) → null, "" (quoted empty) → ""
      :string (let [arr (make-array String n)]
                (dotimes [i n]
                  (aset ^"[Ljava.lang.String;" arr i (nth values i)))
                arr))))

;; ============================================================================
;; Public API
;; ============================================================================

(defn from-csv
  "Read a CSV file into a StratumDataset.

   Returns StratumDataset suitable for queries and table registration.
   String columns are automatically dictionary-encoded for SIMD group-by.
   Uses PostgreSQL NULL semantics: unquoted empty → NULL, quoted empty → \"\".

   Options:
     :separator  — field separator char (default \\,)
     :header?    — first row is header (default true)
     :types      — {\"col\" :long/:double/:string} override auto-detection
     :limit      — max rows to read
     :name       — dataset name (default: derived from filename)"
  [path & {:keys [separator header? types limit name]
           :or {separator \, header? true}}]
  (with-open [reader (io/reader path)]
    (let [rows (read-csv reader :separator separator)
          [headers data-rows] (if header?
                                [(mapv #(or % "") (first rows)) (rest rows)]
                                (let [ncols (count (first rows))]
                                  [(mapv #(str "col" %) (range ncols)) rows]))
          data-rows (if limit (take limit data-rows) data-rows)
          data-rows (vec data-rows)
          ncols (count headers)
          ;; Transpose: extract column vectors
          col-values (mapv (fn [ci]
                             (mapv #(nth % ci nil) data-rows))
                           (range ncols))
          ;; Detect or override types
          col-types (mapv (fn [ci]
                            (if-let [t (get types (nth headers ci))]
                              t
                              (detect-type (nth col-values ci))))
                          (range ncols))
          ;; Build column map
          columns (into {}
                        (map (fn [ci]
                               (let [col-name (keyword (nth headers ci))
                                     col-type (nth col-types ci)
                                     arr (build-column (nth col-values ci) col-type)]
                                 [col-name arr])))
                        (range ncols))
          ;; Derive dataset name from file path
          ds-name (or name (str "csv:" (.getName (io/file path))))]
      (dataset/make-dataset columns
                            {:name ds-name
                             :metadata {:source-path path
                                        :source-type :csv}}))))

(defn from-maps
  "Convert a sequence of maps to a StratumDataset.

   (from-maps [{:name \"Alice\" :age 30} {:name \"Bob\" :age 25}])
   => StratumDataset[\"maps\" 2 rows × 2 cols]

   Values are converted based on the first non-nil value per column:
   - Long/Integer → long[]
   - Double/Float → double[]
   - String → String[] → dictionary-encoded

   Options:
     :name - dataset name (default: \"maps\")"
  ([maps] (from-maps maps {}))
  ([maps {:keys [name] :or {name "maps"}}]
   (let [maps (vec maps)
         n (count maps)
         all-keys (distinct (mapcat keys maps))
         detect-val-type (fn [k]
                           (let [v (some #(get % k) maps)]
                             (cond
                               (nil? v) :string
                               (integer? v) :long
                               (float? v) :double
                               (instance? Double v) :double
                               (string? v) :string
                               :else :string)))
         columns (into {}
                       (map (fn [k]
                              (let [col-type (detect-val-type k)
                                    col-key (if (keyword? k) k (keyword (name k)))
                                    arr (case col-type
                                          :long (let [a (long-array n)]
                                                  (dotimes [i n]
                                                    (let [v (get (nth maps i) k)]
                                                      (aset a i (if (some? v) (long v) Long/MIN_VALUE))))
                                                  a)
                                          :double (let [a (double-array n)]
                                                    (dotimes [i n]
                                                      (let [v (get (nth maps i) k)]
                                                        (aset a i (if (some? v) (double v) Double/NaN))))
                                                    a)
                                          :string (let [a (make-array String n)]
                                                    (dotimes [i n]
                                                      (let [v (get (nth maps i) k)]
                                                        (aset ^"[Ljava.lang.String;" a i
                                                              (when (some? v) (str v)))))
                                                    a))]
                                [col-key arr])))
                       all-keys)]
     (dataset/make-dataset columns
                           {:name name
                            :metadata {:source-type :maps}}))))
