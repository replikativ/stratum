(ns stratum.dataset
  "Native Stratum dataset type - the database table abstraction.

   A dataset is a named collection of typed columns with schema and metadata.
   Datasets are the canonical input/output format for Stratum queries.

   Datasets own branches and persistence. Indices are internal — no branch pointers.
   Implements IEditableCollection/ITransientCollection for Clojure-native
   transient/persistent! lifecycle."
  (:refer-clojure :exclude [resolve load])
  (:require [stratum.column :as column]
            [stratum.index :as idx]
            [stratum.storage :as storage])
  (:import [stratum.index PersistentColumnIndex]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Protocol: Dataset Interface
;; ============================================================================

;; Forward declarations for helpers used inside the StratumDataset
;; deftype but defined further down the file (next to make-dataset /
;; load, where they're also used).
(declare merge-axis-defaults
         materialize-row
         eval-pred
         coerce-temporal-value
         now-in-unit
         validate-period!
         apply-axis-config
         system-now-from-tx-meta
         replace-row-bitemporal!)

(defprotocol IDataset
  "Core dataset protocol for Stratum columnar engine.

   A dataset encapsulates:
   - Named columns with typed data
   - Schema (types, nullability)
   - Identity (name, metadata)
   - Optimization hints (indices, statistics)"

  ;; Identity and metadata
  (ds-name [this]
    "Dataset name (string). Used for table registration, display.")

  (metadata [this]
    "User metadata map. Can store source path, creation time, etc.")

  (row-count [this]
    "Total number of rows (long).")

  ;; Schema
  (column-names [this]
    "Sequence of column keywords in dataset.")

  (schema [this]
    "Schema map: {col-kw {:type :int64|:float64 :nullable? boolean}}
     Future: constraints, primary keys, etc.")

  ;; Column access (query engine integration)
  (columns [this]
    "Return normalized columns map for query execution.

     Format: {col-kw {:type :int64|:float64
                      :data long[]|double[]
                      :source :index (optional)
                      :index PersistentColumnIndex (optional)
                      :dict String[] (optional for string cols)
                      :dict-type :string (if :dict present)}}

     IMPORTANT: This map is already normalized via encode-column.
     Query engine can use it directly without prepare-columns.")

  (column [this col-name]
    "Get single normalized column by name.
     Returns same format as value in columns map.")

  (column-type [this col-name]
    "Return column datatype: :int64 or :float64")

  ;; Optimization hints
  (has-index? [this col-name]
    "True if column is backed by PersistentColumnIndex.
     Index-backed columns enable zero-copy operations and zone map pruning.")

  (chunk-stats [this col-name]
    "Return seq of ChunkStats for zone map optimization.
     Nil if column is not indexed (array-backed).")

  ;; Lifecycle: Fork
  (fork [this]
    "O(1) fork — all index columns forked via idx-fork.
     Only works on datasets with all index-backed columns.
     Returns new dataset with forked columns, cleared commit metadata.")

  ;; Transient mutation
  (set-at! [this col-name row val]
    "Set value at row position in a column. Must be transient.
     Cell-level — bypasses temporal semantics. Use upsert!/retract!
     for SCD2 close-and-reopen.")

  (append! [this row-map] [this row-map tx-meta]
    "Append row values across all columns. Must be transient.
     row-map: {col-kw value}.

     For bitemporal datasets (`:metadata {:bitemporal {...}}`), the
     2-arg form takes a `tx-meta` map carrying `:valid-from`,
     `:valid-to`, `:system-from`, `:system-to`. Any axis column
     missing from `row-map` is auto-stamped from the corresponding
     tx-meta key or from defaults:
       :valid-from  → now-in-axis-unit
       :valid-to    → Long/MAX_VALUE
       :system-from → now-in-axis-unit
       :system-to   → Long/MAX_VALUE
     The 1-arg form behaves like the 2-arg form with `nil` tx-meta.")

  (upsert! [this opts] [this opts tx-meta]
    "SCD2 close-and-reopen on a bitemporal dataset. Must be transient.

     opts: {:where <predicate> :set {col-kw value ...}}
       :where  - row predicate; either a vector clause
                 (e.g. [[:= :eid 1]]) or a function `(fn [row-map])`
       :set    - the new column values for the appended row;
                 columns omitted here inherit from the previous
                 (closed) row, so partial updates work.

     For every matching row whose `:valid` axis is currently open
     (`_valid_to = Long/MAX_VALUE`), closes it to the tx-meta
     `:valid-from`, then appends a new row with the merged values +
     the new vt-window. The new row's axis columns are auto-stamped
     just like `append!`.

     Throws when the dataset is not bitemporal (no `:valid` axis).")

  (retract! [this opts] [this opts tx-meta]
    "Close-without-reopen on a bitemporal dataset. Must be transient.

     opts: {:where <predicate>}

     For every matching open row, closes its `_valid_to` to the
     tx-meta `:valid-from`. Does not append. Logically a retraction
     over the specified vt slice. Physical purge stays on
     `:db/purge`-style semantics — never auto-triggered here.

     Throws when the dataset is not bitemporal.")

  (ds-delete-rows! [this row-idxs]
    "Physically remove the rows at the given indices from a transient
     dataset. Fans `idx-delete!` across every column. Indices are
     sorted descending internally so the unsorted input is fine.
     Decrements row-count. Returns the same transient.

     This bypasses temporal semantics — a posted/sealed dataset
     should not call this directly; prefer `upsert!`/`retract!` with
     `:auto-split? true`. Exposed because the SQL `DELETE` lowering
     and the auto-split overlap handler both need it.")

  ;; Persistence
  (sync! [this store branch]
    "Atomically persist dataset + all indices to storage.
     Returns new dataset with commit metadata.")

  (dirty? [this]
    "True if any index column has unsaved changes."))

;; ============================================================================
;; Implementation: StratumDataset (deftype with volatile fields)
;; ============================================================================

(defn- all-index-backed?
  "Check if all columns in the columns map are index-backed."
  [columns]
  (every? (fn [[_k v]] (= :index (:source v))) columns))

(defn- validate-all-indices!
  "Throw if any columns are array-backed (not index-backed)."
  [columns op-name]
  (when-not (all-index-backed? columns)
    (throw (ex-info (str op-name " requires all columns to be index-backed (not arrays)")
                    {:array-columns (vec (keep (fn [[k v]]
                                                 (when-not (= :index (:source v)) k))
                                               columns))}))))

(deftype StratumDataset
         [ds-name-field                          ; String
          ^:volatile-mutable columns-field       ; {col-kw {:type :source :index ...}}
          schema-field                           ; {col-kw {:type :nullable? ...}}
          ^:volatile-mutable ^long row-count-val ; mutable for transient append!
          ds-metadata-field                      ; map
          ^:volatile-mutable edit                ; Object or nil (transient flag)
          ^:volatile-mutable commit-info-field   ; {:id uuid :branch str} or nil
          obj-meta]                              ; Clojure metadata map

  IDataset
  (ds-name [_] ds-name-field)

  (metadata [_] ds-metadata-field)

  (row-count [_] row-count-val)

  (column-names [_] (keys columns-field))

  (schema [_] schema-field)

  (columns [_] columns-field)

  (column [_ col-name] (get columns-field col-name))

  (column-type [_ col-name]
    (when-let [col (get columns-field col-name)]
      (:type col)))

  (has-index? [_ col-name]
    (when-let [col (get columns-field col-name)]
      (= :index (:source col))))

  (chunk-stats [_ col-name]
    (when-let [col (get columns-field col-name)]
      (when-let [index (:index col)]
        (idx/idx-all-chunk-stats index))))

  ;; ========================================================================
  ;; Lifecycle: Fork
  ;; ========================================================================

  (fork [this]
    (validate-all-indices! columns-field "fork")
    (let [forked-columns
          (into {}
                (map (fn [[col-name col-data]]
                       [col-name (assoc col-data :index (idx/idx-fork (:index col-data)))]))
                columns-field)]
      ;; Preserve commit-info so sync! can establish parent chain
      (StratumDataset. ds-name-field forked-columns schema-field row-count-val
                       ds-metadata-field nil commit-info-field obj-meta)))

  ;; ========================================================================
  ;; Transient Mutation
  ;; ========================================================================

  (set-at! [this col-name row val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent dataset. Call transient first.")))
    (when-let [col (get columns-field col-name)]
      (idx/idx-set! (:index col) row val))
    this)

  (append! [this row-map]
    (append! this row-map nil))

  (append! [this row-map tx-meta]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent dataset. Call transient first.")))
    (let [bt-cfg (when (or (:bitemporal ds-metadata-field) tx-meta)
                   ;; Re-compute the config from metadata to avoid taking the
                   ;; bitemporal-config function dependency on `this`.
                   (let [bt (:bitemporal ds-metadata-field)]
                     (cond-> {}
                       (:valid bt)  (assoc :valid  (merge {:unit :micros} (:valid bt)))
                       (:system bt) (assoc :system (merge {:unit :micros} (:system bt))))))
          ;; Auto-stamp configured axis columns from tx-meta + defaults.
          row (if bt-cfg
                (merge-axis-defaults row-map bt-cfg tx-meta)
                row-map)]
      ;; Reject zero-width / reverse windows BEFORE we touch any column.
      ;; XTDB v2 enforces vf < vt at every entry point; we do the same.
      (when-let [{:keys [from-col to-col]} (:valid bt-cfg)]
        (validate-period! (long (get row from-col)) (long (get row to-col)) :valid))
      (when-let [{:keys [from-col to-col]} (:system bt-cfg)]
        (validate-period! (long (get row from-col)) (long (get row to-col)) :system))
      (doseq [[col-name col-data] columns-field]
        (let [val (get row col-name)]
          (when (nil? val)
            (throw (ex-info "append! requires values for all columns"
                            {:missing col-name
                             :columns (keys columns-field)
                             :hint (when bt-cfg
                                     "configured bitemporal axis columns are auto-stamped from tx-meta or now() — supply them in row-map or pass tx-meta")})))
          (idx/idx-append! (:index col-data) val)))
      (set! row-count-val (unchecked-inc row-count-val))
      this))

  (upsert! [this opts] (upsert! this opts nil))

  (upsert! [this {:keys [where set auto-split?] :as opts} tx-meta]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent dataset. Call transient first.")))
    (let [bt (:bitemporal ds-metadata-field)
          valid-cfg (when (:valid bt) (merge {:unit :micros} (:valid bt)))
          system-cfg (when (:system bt) (merge {:unit :micros} (:system bt)))]
      (when-not valid-cfg
        (throw (ex-info "upsert! requires a :valid axis on the dataset's :bitemporal config"
                        {:metadata ds-metadata-field})))
      (when (nil? where)
        (throw (ex-info "upsert! requires :where" {:opts opts})))
      (let [{vf-col :from-col vt-col :to-col vt-unit :unit} valid-cfg
            close-vt-val (or (coerce-temporal-value (:valid-from tx-meta) vt-unit)
                             (now-in-unit vt-unit))
            n (long row-count-val)
            ;; Pass 1: classify every matching row. A row is :close-safe
            ;; iff it's currently open (vt=MAX) AND its vf is strictly
            ;; before the new write's vf — i.e. closing it to new-vf
            ;; produces a non-empty, non-backwards window. Anything else
            ;; that matches the predicate has a vt-window overlapping
            ;; the new write's [new-vf, MAX) range.
            ;;
            ;; SQL:2011 "application-time-period tables" allow either
            ;; rejecting overlapping writes (the conservative default
            ;; we ship) or surgically splitting historical rows so the
            ;; new write slots in (the `:auto-split?` opt-in, staged
            ;; for a follow-up). Auto-split mirrors XTDB v2's
            ;; without-overlaps invariant in concept; the precise
            ;; semantics here are determined by the predicate language
            ;; and the SCD2 row layout chosen by the dataset config.
            ;; A matching row falls into one of three classes vs the
            ;; new write's [new-vf, MAX) window:
            ;;   :close-safe — open row whose vf < new-vf; we close it
            ;;                 to new-vf and append a new row.
            ;;   :no-conflict — closed row entirely in the past
            ;;                  (row-vt <= new-vf). Ignored.
            ;;   :overlap     — anything else; can't proceed without
            ;;                  auto-split.
            {:keys [close-safe overlaps]}
            (loop [i 0 acc {:close-safe (transient [])
                            :overlaps (transient [])}]
              (if (>= i n)
                {:close-safe (persistent! (:close-safe acc))
                 :overlaps   (persistent! (:overlaps acc))}
                (let [row (materialize-row columns-field i)]
                  (if (eval-pred where row)
                    (let [row-vf (long (get row vf-col))
                          row-vt (long (get row vt-col))
                          open?  (= row-vt Long/MAX_VALUE)
                          klass (cond
                                  (and open? (< row-vf close-vt-val)) :close-safe
                                  ;; Closed row entirely before new-vf — no overlap
                                  (and (not open?) (<= row-vt close-vt-val)) :no-conflict
                                  :else :overlaps)]
                      (recur (inc i)
                             (if (= :no-conflict klass)
                               acc
                               (update acc klass conj! [i row]))))
                    (recur (inc i) acc)))))
        ;; Auto-split partitions overlaps by row-vf vs new-vf:
        ;;   row-vf <  new-vf → partial left overlap → TRUNCATE
        ;;                       (set row-vt = new-vf, keep the
        ;;                        historical prefix)
        ;;   row-vf >= new-vf → row entirely inside [new-vf, MAX) →
        ;;                       DROP (physical removal via
        ;;                        ds-delete-rows!).
        ;; (Right-partial and middle-overlap don't occur here because
        ;; new-vt is always MAX in upsert!/retract!. They become
        ;; reachable once Phase D wires SQL `FOR PORTION OF
        ;; VALID_TIME FROM x TO y`.)
        {auto-truncate :truncate auto-drop :drop}
        (if (and (seq overlaps) auto-split?)
          (group-by (fn [[_i r]]
                      (if (< (long (get r vf-col)) close-vt-val)
                        :truncate
                        :drop))
                    overlaps)
          {:truncate [] :drop []})

        _ (when (and (seq overlaps) (not auto-split?))
            (throw (ex-info "upsert! would overlap existing rows' vt-windows"
                            {:where where
                             :new-window [close-vt-val Long/MAX_VALUE]
                             :overlaps (mapv (fn [[i r]]
                                               (assoc (select-keys r [vf-col vt-col]) :row-idx i))
                                             overlaps)
                             :hint "pass :auto-split? true to split overlapping rows, or restructure the write"})))]
        ;; Mutation order is load-bearing:
        ;;   1. set-at! everything (close-safe + truncate) — indices stay
        ;;      stable across set-at!.
        ;;   2. ds-delete-rows! for drops — this shifts indices, so any
        ;;      later references to the captured indices (close-safe,
        ;;      truncate) would point at the wrong row.
        ;;   3. append! the new rows (close-safe merged-row, or degenerate
        ;;      insert).
        (if system-cfg
          ;; ----- Bitemporal path (SCD2-on-both-axes) -----
          ;; Every mutation of a row's vt-window is also a system-time
          ;; event: close the old row's _system_to, append the
          ;; replacement(s) with a fresh _system_from. This preserves
          ;; the audit chain so `FOR SYSTEM_TIME AS OF <past>` still
          ;; sees the pre-surgery state.
          (let [system-now (system-now-from-tx-meta system-cfg tx-meta)]
            (doseq [[i prev-row] close-safe]
              (let [orig-vf (long (get prev-row vf-col))]
                (replace-row-bitemporal!
                  this prev-row i system-now valid-cfg system-cfg
                  [{:vf orig-vf :vt close-vt-val}
                   {:vf close-vt-val :vt Long/MAX_VALUE :data set}]
                  tx-meta)))
            (doseq [[i prev-row] auto-truncate]
              (let [orig-vf (long (get prev-row vf-col))]
                (replace-row-bitemporal!
                  this prev-row i system-now valid-cfg system-cfg
                  [{:vf orig-vf :vt close-vt-val}]
                  tx-meta)))
            (doseq [[i prev-row] auto-drop]
              (replace-row-bitemporal!
                this prev-row i system-now valid-cfg system-cfg
                []
                tx-meta))
            (when (empty? close-safe)
              (append! this set
                       (assoc tx-meta :valid-from close-vt-val
                              :system-from system-now))))
          ;; ----- Valid-only path (existing in-place mutation) -----
          (do
            (doseq [[i _prev-row] close-safe]
              (idx/idx-set! (:index (get columns-field vt-col)) i close-vt-val))
            (doseq [[i _r] auto-truncate]
              (idx/idx-set! (:index (get columns-field vt-col)) i close-vt-val))
            (when (seq auto-drop)
              (ds-delete-rows! this (mapv first auto-drop)))
            (doseq [[_i prev-row] close-safe]
              (let [merged-row (merge (dissoc prev-row vf-col vt-col)
                                      set)]
                (append! this
                         (assoc merged-row vf-col close-vt-val)
                         tx-meta)))
            (when (empty? close-safe)
              (append! this set (assoc tx-meta :valid-from close-vt-val)))))
        this)))

  (retract! [this opts] (retract! this opts nil))

  (retract! [this {:keys [where auto-split?] :as opts} tx-meta]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent dataset. Call transient first.")))
    (let [bt (:bitemporal ds-metadata-field)
          valid-cfg (when (:valid bt) (merge {:unit :micros} (:valid bt)))
          system-cfg (when (:system bt) (merge {:unit :micros} (:system bt)))]
      (when-not valid-cfg
        (throw (ex-info "retract! requires a :valid axis on the dataset's :bitemporal config"
                        {:metadata ds-metadata-field})))
      (when (nil? where)
        (throw (ex-info "retract! requires :where" {:opts opts})))
      (let [{vf-col :from-col vt-col :to-col vt-unit :unit} valid-cfg
            close-vt-val (or (coerce-temporal-value (:valid-from tx-meta) vt-unit)
                             (now-in-unit vt-unit))
            close-vt-end (or (coerce-temporal-value (:valid-to tx-meta) vt-unit)
                             Long/MAX_VALUE)
            _ (when (not= close-vt-end Long/MAX_VALUE)
                ;; Bounded retract has explicit [vf, vt) — reject
                ;; zero-width / reverse periods that would silently
                ;; degenerate the overlap test to a no-op.
                (validate-period! close-vt-val close-vt-end :valid))
            ;; Bounded retract — SQL:2011 `FOR PORTION OF VALID_TIME
            ;; FROM x TO y DELETE` semantic. For each matching row's
            ;; window [row-vf, row-vt), apply surgery against
            ;; [new-vf, new-vt):
            ;;   row fully inside  → drop
            ;;   partial left      → truncate vt to new-vf
            ;;   partial right     → shift vf forward to new-vt
            ;;   straddles both    → split (truncate + append right tail)
            ;; Bounded retract is unconditional — no `:auto-split?` flag,
            ;; the bounded form IS the surgical semantic.
            bounded? (not= close-vt-end Long/MAX_VALUE)
            n (long row-count-val)]
        (if bounded?
          ;; ----- Bounded retract (SQL:2011 FOR PORTION OF VALID_TIME) -----
          ;; Surgery is unconditional — no `:auto-split?` flag.
          (let [{:keys [drops trunc-vt trunc-vf splits]}
                (loop [i 0 acc {:drops (transient [])
                                :trunc-vt (transient [])
                                :trunc-vf (transient [])
                                :splits (transient [])}]
                  (if (>= i n)
                    {:drops    (persistent! (:drops acc))
                     :trunc-vt (persistent! (:trunc-vt acc))
                     :trunc-vf (persistent! (:trunc-vf acc))
                     :splits   (persistent! (:splits acc))}
                    (let [row (materialize-row columns-field i)]
                      (if (eval-pred where row)
                        (let [row-vf (long (get row vf-col))
                              row-vt (long (get row vt-col))
                              klass (cond
                                      (or (<= row-vt close-vt-val)
                                          (>= row-vf close-vt-end))
                                      :no-conflict

                                      (and (>= row-vf close-vt-val)
                                           (<= row-vt close-vt-end))
                                      :drops

                                      (and (<  row-vf close-vt-val)
                                           (<= row-vt close-vt-end))
                                      :trunc-vt

                                      (and (>= row-vf close-vt-val)
                                           (>  row-vt close-vt-end))
                                      :trunc-vf

                                      :else :splits)]
                          (recur (inc i)
                                 (if (= :no-conflict klass)
                                   acc
                                   (update acc klass conj! [i row]))))
                        (recur (inc i) acc)))))]
            (if system-cfg
              ;; ----- Bitemporal bounded retract -----
              ;; Each surgery class gets its system-time event: close
              ;; old row's system-to, append the surviving slice(s)
              ;; with fresh system-from. Drops have no replacements.
              (let [system-now (system-now-from-tx-meta system-cfg tx-meta)]
                (doseq [[i prev-row] drops]
                  (replace-row-bitemporal!
                    this prev-row i system-now valid-cfg system-cfg
                    []
                    tx-meta))
                (doseq [[i prev-row] trunc-vt]
                  (let [orig-vf (long (get prev-row vf-col))]
                    (replace-row-bitemporal!
                      this prev-row i system-now valid-cfg system-cfg
                      [{:vf orig-vf :vt close-vt-val}]
                      tx-meta)))
                (doseq [[i prev-row] trunc-vf]
                  (let [orig-vt (long (get prev-row vt-col))]
                    (replace-row-bitemporal!
                      this prev-row i system-now valid-cfg system-cfg
                      [{:vf close-vt-end :vt orig-vt}]
                      tx-meta)))
                (doseq [[i prev-row] splits]
                  (let [orig-vf (long (get prev-row vf-col))
                        orig-vt (long (get prev-row vt-col))]
                    (replace-row-bitemporal!
                      this prev-row i system-now valid-cfg system-cfg
                      [{:vf orig-vf :vt close-vt-val}
                       {:vf close-vt-end :vt orig-vt}]
                      tx-meta)))
                this)
              ;; ----- Valid-only bounded retract (existing in-place) -----
              (do
                (doseq [[i _r] trunc-vt]
                  (idx/idx-set! (:index (get columns-field vt-col)) i close-vt-val))
                (doseq [[i _r] trunc-vf]
                  (idx/idx-set! (:index (get columns-field vf-col)) i close-vt-end))
                (let [tails (mapv (fn [[i r]]
                                    (idx/idx-set! (:index (get columns-field vt-col))
                                                  i close-vt-val)
                                    (-> r (assoc vf-col close-vt-end)))
                                  splits)]
                  (when (seq drops)
                    (ds-delete-rows! this (mapv first drops)))
                  (doseq [tail tails]
                    (append! this tail tx-meta)))
                this)))
          ;; ----- Open-window retract (existing behavior) -----
          (let [{:keys [close-safe overlaps]}
                (loop [i 0 acc {:close-safe (transient [])
                                :overlaps (transient [])}]
                  (if (>= i n)
                    {:close-safe (persistent! (:close-safe acc))
                     :overlaps   (persistent! (:overlaps acc))}
                    (let [row (materialize-row columns-field i)]
                      (if (eval-pred where row)
                        (let [row-vf (long (get row vf-col))
                              row-vt (long (get row vt-col))
                              open?  (= row-vt Long/MAX_VALUE)
                              klass (cond
                                      (and open? (< row-vf close-vt-val)) :close-safe
                                      (and (not open?) (<= row-vt close-vt-val)) :no-conflict
                                      :else :overlaps)]
                          (recur (inc i)
                                 (if (= :no-conflict klass)
                                   acc
                                   (update acc klass conj! [i row]))))
                        (recur (inc i) acc)))))
                {auto-truncate :truncate auto-drop :drop}
                (if (and (seq overlaps) auto-split?)
                  (group-by (fn [[_i r]]
                              (if (< (long (get r vf-col)) close-vt-val)
                                :truncate
                                :drop))
                            overlaps)
                  {:truncate [] :drop []})]
            (when (and (seq overlaps) (not auto-split?))
              (throw (ex-info "retract! would touch rows whose vt-window doesn't cover the retract instant"
                              {:where where
                               :retract-at close-vt-val
                               :overlaps (mapv (fn [[i r]]
                                                 (assoc (select-keys r [vf-col vt-col]) :row-idx i))
                                               overlaps)
                               :hint "pass :auto-split? true to split overlapping rows, or restructure the write"})))
            (if system-cfg
              ;; ----- Bitemporal open-window retract -----
              (let [system-now (system-now-from-tx-meta system-cfg tx-meta)]
                (doseq [[i prev-row] close-safe]
                  (let [orig-vf (long (get prev-row vf-col))]
                    (replace-row-bitemporal!
                      this prev-row i system-now valid-cfg system-cfg
                      [{:vf orig-vf :vt close-vt-val}]
                      tx-meta)))
                (doseq [[i prev-row] auto-truncate]
                  (let [orig-vf (long (get prev-row vf-col))]
                    (replace-row-bitemporal!
                      this prev-row i system-now valid-cfg system-cfg
                      [{:vf orig-vf :vt close-vt-val}]
                      tx-meta)))
                (doseq [[i prev-row] auto-drop]
                  (replace-row-bitemporal!
                    this prev-row i system-now valid-cfg system-cfg
                    []
                    tx-meta))
                this)
              ;; ----- Valid-only open-window retract -----
              (do
                (doseq [[i _row] close-safe]
                  (idx/idx-set! (:index (get columns-field vt-col)) i close-vt-val))
                (doseq [[i _r] auto-truncate]
                  (idx/idx-set! (:index (get columns-field vt-col)) i close-vt-val))
                (when (seq auto-drop)
                  (ds-delete-rows! this (mapv first auto-drop)))
                this)))))))

  (ds-delete-rows! [this row-idxs]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent dataset. Call transient first.")))
    (let [n (long row-count-val)
          ;; Distinct + descending: descending so each delete doesn't
          ;; shift the still-pending indices above it. Distinct so a
          ;; duplicated index doesn't double-delete (which would target
          ;; the row that just shifted into its place).
          sorted (vec (sort > (distinct row-idxs)))]
      (doseq [i sorted]
        (when (or (neg? (long i)) (>= (long i) n))
          (throw (IndexOutOfBoundsException.
                  (str "ds-delete-rows!: index " i " out of bounds [0, " n ")")))))
      (doseq [i sorted]
        (doseq [[_col-name col-data] columns-field]
          (idx/idx-delete! (:index col-data) (long i))))
      (set! row-count-val (- n (count sorted)))
      this))

  ;; ========================================================================
  ;; Persistence
  ;; ========================================================================

  (sync! [this store branch]
    (when edit
      (throw (IllegalStateException. "Cannot sync transient dataset. Call persistent! first.")))
    (validate-all-indices! columns-field "sync!")

    ;; Wrap in storage lock to prevent concurrent GC from deleting freshly written chunks
    (storage/with-storage-lock
      (fn []
        ;; 1. Sync each index column (no branch)
        (let [synced-columns
              (into {}
                    (map (fn [[col-name col-data]]
                           (let [index (:index col-data)
                                 synced-index (idx/idx-sync! index store)
                                 idx-commit-id (get-in (meta synced-index) [:commit :id])]
                             [col-name (assoc col-data
                                              :index synced-index
                                              :index-commit idx-commit-id)])))
                    columns-field)

              ;; 2. Build dataset snapshot
              column-commits (into {}
                                   (map (fn [[col-name col-data]]
                                          [col-name (cond-> {:index-commit (:index-commit col-data)
                                                             :type (:type col-data)}
                                                      ;; Persist dict info so load can restore it
                                                      (:dict col-data)
                                                      (assoc :dict (vec (:dict col-data))
                                                             :dict-type (:dict-type col-data)))]))
                                   synced-columns)
              parent-commit (:id commit-info-field)
              parents (if parent-commit #{parent-commit} #{})
              crypto-hash? (:crypto-hash? ds-metadata-field)
              dataset-commit-id (if crypto-hash?
                                  (storage/generate-commit-id
                                   {:columns column-commits
                                    :schema schema-field
                                    :metadata ds-metadata-field})
                                  (storage/generate-commit-id))
              ds-snapshot {:dataset-id dataset-commit-id
                           :name ds-name-field
                           :branch branch
                           :parents parents
                           :columns column-commits
                           :schema schema-field
                           :row-count row-count-val
                           :metadata ds-metadata-field
                           :timestamp (System/currentTimeMillis)}]

          ;; 3. Write dataset commit
          (storage/write-dataset-commit! store dataset-commit-id ds-snapshot)

          ;; 4. Update branch HEAD
          (storage/update-dataset-head! store branch dataset-commit-id)

          ;; 5. Register branch
          (storage/register-dataset-branch! store branch)

          ;; 6. Return new dataset with commit metadata
          (let [new-commit-info {:id dataset-commit-id :branch branch}]
            (StratumDataset. ds-name-field synced-columns schema-field row-count-val
                             ds-metadata-field nil new-commit-info obj-meta))))))

  (dirty? [_]
    (some (fn [[_col-name col-data]]
            (when (= :index (:source col-data))
              (pos? (count (idx/idx-dirty-chunks (:index col-data))))))
          columns-field))

  ;; ========================================================================
  ;; Clojure Standard Protocols
  ;; ========================================================================

  clojure.lang.Seqable
  (seq [_] (seq columns-field))

  clojure.lang.IPersistentCollection
  (count [_] (count columns-field))
  (cons [_ _] (throw (UnsupportedOperationException. "Use make-dataset")))
  (empty [_] (throw (UnsupportedOperationException. "Use make-dataset")))
  (equiv [this o] (identical? this o))

  clojure.lang.IObj
  (meta [_] obj-meta)
  (withMeta [_ m]
    (StratumDataset. ds-name-field columns-field schema-field row-count-val
                     ds-metadata-field edit commit-info-field m))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [_ k not-found]
    (case k
      :name ds-name-field
      :columns columns-field
      :schema schema-field
      :row-count row-count-val
      :metadata ds-metadata-field
      :commit-info commit-info-field
      :transient? (some? edit)
      not-found))

  clojure.lang.IPersistentMap
  (assoc [this k v]
    "Add or update a column. Auto-converts data to index via encode-column.
     Validates length matches dataset row count.
     Throws on transient datasets - use set-at!/append! instead."
    (when edit
      (throw (IllegalStateException.
              "Cannot assoc on transient dataset. Use set-at! or append! instead.")))

    (let [encoded-col (column/encode-column v)
          n row-count-val

          ;; Get new column length
          new-col-length (if (= :index (:source encoded-col))
                           (idx/idx-length (:index encoded-col))
                           (let [arr (:data encoded-col)]
                             (if (instance? (Class/forName "[D") arr)
                               (alength ^doubles arr)
                               (alength ^longs arr))))]

      ;; Validate length (allow empty dataset to establish row count)
      (when (and (pos? n) (not= new-col-length n))
        (throw (ex-info
                (str "Column length mismatch: dataset has " n
                     " rows, new column has " new-col-length " rows")
                {:dataset-rows n
                 :column-rows new-col-length
                 :column-name k})))

      ;; Create new dataset with added column
      (let [new-columns (assoc columns-field k encoded-col)
            new-schema (assoc schema-field k {:type (:type encoded-col)
                                              :nullable? true})
            new-row-count (if (zero? n) new-col-length n)]
        (StratumDataset. ds-name-field new-columns new-schema new-row-count
                         ds-metadata-field nil nil obj-meta))))

  (without [this k]
    "Remove a column. Returns new dataset without the column.
     Throws on transient datasets."
    (when edit
      (throw (IllegalStateException.
              "Cannot dissoc on transient dataset.")))

    (if (contains? columns-field k)
      (let [new-columns (dissoc columns-field k)
            new-schema (dissoc schema-field k)]
        (StratumDataset. ds-name-field new-columns new-schema row-count-val
                         ds-metadata-field nil nil obj-meta))
      this))  ;; Return unchanged if column doesn't exist

  (containsKey [_ k]
    (contains? columns-field k))

  (entryAt [_ k]
    (when-let [col-data (get columns-field k)]
      (clojure.lang.MapEntry. k col-data)))

  clojure.lang.IEditableCollection
  (asTransient [this]
    (when edit
      (throw (IllegalStateException. "Already transient")))
    (validate-all-indices! columns-field "transient")
    ;; Return a NEW transient dataset, leaving this persistent instance unchanged
    (let [transient-columns
          (into {}
                (map (fn [[col-name col-data]]
                       [col-name (assoc col-data :index (idx/idx-transient (:index col-data)))]))
                columns-field)]
      (StratumDataset. ds-name-field transient-columns schema-field row-count-val
                       ds-metadata-field (Object.) commit-info-field obj-meta)))

  clojure.lang.ITransientCollection
  (persistent [this]
    (when-not edit
      (throw (IllegalStateException. "Already persistent")))
    ;; Seal all index columns persistent
    (let [persistent-columns
          (into {}
                (map (fn [[col-name col-data]]
                       [col-name (assoc col-data :index (idx/idx-persistent! (:index col-data)))]))
                columns-field)]
      (set! columns-field persistent-columns)
      (set! edit nil)
      this))
  (conj [_ _]
    (throw (UnsupportedOperationException. "Use append! instead")))

  Object
  (toString [_]
    (str "#StratumDataset["
         (pr-str ds-name-field)
         " "
         row-count-val
         " rows × "
         (count columns-field)
         " cols"
         (when-let [indexed-cols (seq (filter (fn [[_k v]] (= :index (:source v))) columns-field))]
           (str " (indexed: " (count indexed-cols) ")"))
         (when edit " TRANSIENT")
         "]")))

;; ============================================================================
;; Custom Printing
;; ============================================================================

(defmethod print-method StratumDataset [^StratumDataset ds ^java.io.Writer w]
  (.write w (.toString ds)))

;; ============================================================================
;; Standalone function: transient?
;; ============================================================================

(defn transient?
  "True if dataset is in transient mode."
  [ds]
  (:transient? ds))

;; ============================================================================
;; Schema Inference
;; ============================================================================

(defn- infer-schema
  "Infer schema from normalized columns map."
  [columns]
  (into {}
        (map (fn [[col-name col-data]]
               [col-name {:type (:type col-data)
                          :nullable? true}]))
        columns))

;; ============================================================================
;; Bitemporal configuration
;;
;; A dataset opts into bitemporal semantics via
;;   :metadata {:bitemporal {:valid  {:from-col <kw> :to-col <kw> :unit U}
;;                           :system {:from-col <kw> :to-col <kw> :unit U}}}
;; Either axis is optional; both are processed identically. See
;; doc/temporal-design.md for the full design and rationale.
;; ============================================================================

(defn- apply-axis-config
  "Validate one bitemporal axis (`:valid` or `:system`) and stamp the
   two named columns with `:temporal-unit unit`. Axis-kw is just for
   error messages. Throws on missing column ref, non-int64 column
   type, or conflicting pre-existing `:temporal-unit`."
  [cols axis-kw axis-spec]
  (let [{:keys [from-col to-col]
         unit :unit
         :or  {unit :micros}} axis-spec
        tag (fn [col-name]
              (let [c (get cols col-name)]
                (when (nil? c)
                  (throw (ex-info (str ":bitemporal " axis-kw " references missing column")
                                  {:axis axis-kw
                                   :config axis-spec
                                   :missing col-name
                                   :columns (vec (keys cols))})))
                (when (not= :int64 (:type c))
                  (throw (ex-info (str ":bitemporal " axis-kw " column must be :int64")
                                  {:axis axis-kw
                                   :config axis-spec
                                   :column col-name
                                   :type (:type c)})))
                (when (and (:temporal-unit c) (not= (:temporal-unit c) unit))
                  (throw (ex-info (str ":bitemporal " axis-kw " unit conflicts with column :temporal-unit")
                                  {:axis axis-kw
                                   :config axis-spec
                                   :column col-name
                                   :existing-unit (:temporal-unit c)
                                   :config-unit unit})))
                (assoc c :temporal-unit unit)))]
    (assoc cols
           from-col (tag from-col)
           to-col   (tag to-col))))

(defn- apply-bitemporal-config
  "If `metadata` carries a `:bitemporal` config, stamp the configured
   axes' columns with `:temporal-unit`. Called from both `make-dataset`
   (first construction) and `load` (restore from storage), so the
   `:temporal-unit` tag is always present when downstream consumers
   read a bitemporal dataset. Metadata is the source of truth since
   per-column commit payloads don't persist `:temporal-unit`."
  [cols metadata]
  (let [bt (:bitemporal metadata)]
    (cond-> cols
      (:valid  bt) (apply-axis-config :valid  (:valid  bt))
      (:system bt) (apply-axis-config :system (:system bt)))))

(defn bitemporal-config
  "Return the validated `:bitemporal` config map from a dataset's
   metadata, or `nil` if the dataset is not bitemporal. Each axis
   spec gets `:unit` defaulted to `:micros` if absent. Used by
   adapters and the planner to discover which columns form the
   valid-time / system-time windows without parsing names."
  [ds]
  (when-let [bt (:bitemporal (:metadata ds))]
    (cond-> {}
      (:valid bt)  (assoc :valid  (merge {:unit :micros} (:valid bt)))
      (:system bt) (assoc :system (merge {:unit :micros} (:system bt))))))

(defn valid-time-config
  "Return just the validated `:valid` axis spec, or nil if the dataset
   has no valid-time axis. Convenience for callers that only care
   about the valid-time window (the common case for SCD2 adapters)."
  [ds]
  (:valid (bitemporal-config ds)))

(defn system-time-config
  "Return just the validated `:system` axis spec, or nil if the
   dataset has no system-time axis."
  [ds]
  (:system (bitemporal-config ds)))

;; ============================================================================
;; Temporal write helpers
;; ============================================================================

(defn- now-in-unit
  "Current wall-clock time as a long in the requested `:temporal-unit`.
   Used to default `:valid-from` / `:system-from` when the caller
   doesn't supply one. `(System/currentTimeMillis)` returns millis;
   multiplying by 1000 gives micros, dividing by 1000 gives seconds,
   etc."
  ^long [unit]
  (let [ms (System/currentTimeMillis)]
    (case unit
      :micros  (* 1000 ms)
      :millis  ms
      :seconds (quot ms 1000)
      :days    (quot ms 86400000)
      ;; Unknown unit — fall back to millis. apply-axis-config rejects
      ;; unknown units at make-dataset time, so this is defensive.
      ms)))

(defn- coerce-temporal-value
  "Coerce a temporal value to a long in the axis' unit. Accepts:
     java.util.Date   → millis-since-epoch converted to unit
     java.time.Instant → millis-since-epoch converted to unit
     Long / int       → already in unit, passthrough
   Returns nil for nil input (caller decides on default)."
  ^Long [v unit]
  (when (some? v)
    (cond
      (instance? java.util.Date v)
      (let [ms (.getTime ^java.util.Date v)]
        (case unit
          :micros  (* 1000 ms)
          :millis  ms
          :seconds (quot ms 1000)
          :days    (quot ms 86400000)
          ms))

      (instance? java.time.Instant v)
      (let [ms (.toEpochMilli ^java.time.Instant v)]
        (case unit
          :micros  (* 1000 ms)
          :millis  ms
          :seconds (quot ms 1000)
          :days    (quot ms 86400000)
          ms))

      (number? v)
      (long v)

      :else
      (throw (ex-info "Unsupported temporal value type"
                      {:value v :type (type v) :unit unit})))))

(defn- validate-period!
  "Reject zero-width and reverse temporal windows. SQL:2011 + XTDB v2
   require `from < to`; stratum's bounded surgery silently degenerates
   to a no-op when `to <= from` (overlap test collapses), so any
   downstream `bounded-update!`/`retract!`/`bounded-INSERT` on an
   invalid window silently swallows the user's intent.

   Pass `axis` (`:valid` or `:system`) for an informative error."
  [^long from ^long to axis]
  (when-not (< from to)
    (throw (ex-info (str "Invalid " (name axis) " window: from >= to "
                         "(zero-width or reverse). "
                         "stratum requires a strictly positive half-open interval [from, to).")
                    {:axis axis :from from :to to}))))

(defn- system-now-from-tx-meta
  "Pick the system-time stamp for a batch of SCD2 mutations. Honors
   `:system-from` in tx-meta (replayable test runs / regulator
   replays) or falls back to `now-in-unit` on the system axis. The
   *same* `system-now` is used to close every old row's system-to AND
   to stamp every successor row's system-from inside a single
   transactional surgery — so AS OF queries get a coherent cut.

   Returns nil when the dataset has no `:system` axis configured."
  [system-cfg tx-meta]
  (when system-cfg
    (let [unit (:unit system-cfg)]
      (or (coerce-temporal-value (:system-from tx-meta) unit)
          (now-in-unit unit)))))

(defn- merge-axis-defaults
  "For each configured axis (`:valid`, `:system`), if `row-map`
   doesn't already contain the axis' from-col / to-col, fill them
   from the matching tx-meta key (`:valid-from` / `:valid-to` /
   `:system-from` / `:system-to`) coerced to the axis' unit, or
   from defaults: now-in-unit for `:_from`, `Long/MAX_VALUE` for
   `:_to`.

   Returns the augmented row-map. Caller's explicit values in
   `row-map` always win over tx-meta values, which always win over
   defaults."
  [row-map bt-cfg tx-meta]
  (cond-> row-map
    (:valid bt-cfg)
    (as-> r
          (let [{:keys [from-col to-col unit]} (:valid bt-cfg)]
            (cond-> r
              (not (contains? r from-col))
              (assoc from-col (or (coerce-temporal-value (:valid-from tx-meta) unit)
                                  (now-in-unit unit)))
              (not (contains? r to-col))
              (assoc to-col (or (coerce-temporal-value (:valid-to tx-meta) unit)
                                Long/MAX_VALUE)))))

    (:system bt-cfg)
    (as-> r
          (let [{:keys [from-col to-col unit]} (:system bt-cfg)]
            (cond-> r
              (not (contains? r from-col))
              (assoc from-col (or (coerce-temporal-value (:system-from tx-meta) unit)
                                  (now-in-unit unit)))
              (not (contains? r to-col))
              (assoc to-col (or (coerce-temporal-value (:system-to tx-meta) unit)
                                Long/MAX_VALUE)))))))

(defn- eval-pred
  "Minimal stratum-style predicate evaluator for `upsert!`/`retract!`
   `:where` clauses. Supports vector predicates `[op col val]` for
   `:= := :< :<= :> :>= :!= :in`, vector `[:and pred ...]` /
   `[:or pred ...]` / `[:not pred]`, OR a Clojure function
   `(fn [row-map] bool)`. Returns truthy/falsy.

   Not a full planner integration — Phase B intentionally keeps this
   in pure Clojure. Stratum-planner pushdown is a Phase C+ task."
  [pred row]
  (cond
    (fn? pred) (pred row)

    (and (vector? pred) (vector? (first pred)))
    ;; Top-level [[op col val] [op col val] ...] is implicit AND
    (every? #(eval-pred % row) pred)

    (vector? pred)
    (let [[op & args] pred]
      (case op
        :=   (= (get row (first args)) (second args))
        :!=  (not= (get row (first args)) (second args))
        :<   (let [v (get row (first args))] (and (some? v) (< v (second args))))
        :<=  (let [v (get row (first args))] (and (some? v) (<= v (second args))))
        :>   (let [v (get row (first args))] (and (some? v) (> v (second args))))
        :>=  (let [v (get row (first args))] (and (some? v) (>= v (second args))))
        :in  (contains? (set (second args)) (get row (first args)))
        :and (every? #(eval-pred % row) args)
        :or  (some #(eval-pred % row) args)
        :not (not (eval-pred (first args) row))
        (throw (ex-info "Unsupported predicate op for upsert!/retract!"
                        {:op op :pred pred}))))

    :else
    (throw (ex-info "Unsupported predicate shape" {:pred pred}))))

(defn- read-col-at
  "Read column value at row position `i`. Handles array-backed
   (`:data`) and index-backed (`:index`) columns plus string-dict
   decoding."
  [col-data ^long i]
  (let [data (:data col-data)
        dict (:dict col-data)
        idx  (:index col-data)]
    (cond
      ;; Dict-encoded string column (data is long[] of codes)
      dict
      (let [^longs arr (or data (idx/idx-materialize-to-array idx))
            code (aget arr i)]
        (when-not (= code Long/MIN_VALUE)
          (aget ^"[Ljava.lang.String;" dict (int code))))

      ;; Raw double array
      (and data (instance? (Class/forName "[D") data))
      (aget ^doubles data i)

      ;; Raw String array
      (and data (instance? (Class/forName "[Ljava.lang.String;") data))
      (aget ^"[Ljava.lang.String;" data i)

      ;; Raw long array
      (and data (instance? (Class/forName "[J") data))
      (aget ^longs data i)

      ;; Index-backed
      idx
      (case (idx/idx-datatype idx)
        :float64 (idx/idx-get-double idx i)
        :int64   (idx/idx-get-long idx i)
        nil)

      :else nil)))

(defn- materialize-row
  "Read row `i` from each column in `cols` as a row-map. Used by
   `upsert!`/`retract!` for predicate evaluation against in-transient
   rows. No intermediate vector allocation."
  [cols ^long i]
  (persistent!
   (reduce-kv
    (fn [m col-name col-data]
      (assoc! m col-name (read-col-at col-data i)))
    (transient {})
    cols)))

;; ============================================================================
;; SCD2-on-both-axes surgery helper.
;;
;; In a bitemporal dataset (both `:valid` AND `:system` axes
;; configured), an SCD2 mutation of an existing row's valid window
;; must ALSO advance the system axis — otherwise a query `FOR
;; SYSTEM_TIME AS OF <past-instant>` would return post-correction
;; data, which collapses the audit story. XTDB v2 achieves this by
;; being event-sourced; we achieve it by closing the old row's
;; `_system_to` in place and appending one or more "replacement"
;; rows carrying the new valid window AND a fresh `_system_from`.
;;
;; For non-bitemporal (valid-only) datasets we keep the existing
;; in-place vt-col mutation behavior — there's no system-time to
;; preserve.
;; ============================================================================

(defn- replace-row-bitemporal!
  "Apply SCD2-on-both-axes surgery for the row at index `i`. The
   row's pre-mutation snapshot is `prev-row` (captured before any
   mutation). Closes the row's `_system_to` to `system-now`, then
   appends one row per `replacement-spec`. Each replacement is a
   map `{:vf x :vt y :data {...}?}`: the appended row inherits
   `prev-row` minus the four axis columns, then gets `vf-col`=vf,
   `vt-col`=vt, and the merged `:data` (if any). `append!` stamps
   the appended row's system axis from `tx-meta :system-from
   system-now`.

   `replacement-specs` may be empty — a pure 'drop in valid-time'
   that still advances system-time on the old row."
  [this prev-row i system-now valid-cfg system-cfg replacement-specs tx-meta]
  (let [{vf-col :from-col vt-col :to-col} valid-cfg
        {sys-from-col :from-col sys-to-col :to-col} system-cfg
        cols (columns this)
        successor-tx-meta (assoc tx-meta :system-from system-now)]
    ;; 1. Close the old row's system-to. The row stays in storage at
    ;;    index `i` with its original vf/vt/data/sys-from intact — so
    ;;    AS OF queries at `system-time < system-now` still see it.
    (idx/idx-set! (:index (get cols sys-to-col)) i system-now)
    ;; 2. Append the replacement(s). Each gets fresh system-from from
    ;;    `successor-tx-meta`; the system-to defaults to MAX.
    (doseq [{:keys [vf vt data]} replacement-specs]
      (let [base (dissoc prev-row vf-col vt-col sys-from-col sys-to-col)
            row  (-> base
                     (merge (or data {}))
                     (assoc vf-col vf vt-col vt))]
        (append! this row successor-tx-meta)))))

;; ============================================================================
;; SQL:2011 bounded-window UPDATE: `UPDATE … FOR PORTION OF VALID_TIME
;; FROM x TO y SET col=val WHERE p`. Decomposes into:
;;   1. capture matching rows that overlap [x, y) (including their
;;      original values and per-row overlap window);
;;   2. retract! over [x, y) — the bounded retract handles the
;;      temporal surgery for both matched and untouched portions;
;;   3. append a new slice per captured row, merging :set into the
;;      original row values and stamping the per-row overlap window.
;; Top-level function (rather than protocol method) since it composes
;; existing primitives — keeps the IDataset surface small.
;; ============================================================================

(defn bounded-update!
  "SQL:2011 non-sequenced UPDATE on a transient bitemporal dataset.

   opts: {:where <predicate> :set {col-kw value ...}}
   tx-meta: must include `:valid-from` AND `:valid-to`; the bounded
            surgery applies over [`valid-from`, `valid-to`).

   For every matching row whose vt-window overlaps the bounded slice,
   the slice's overlap portion gets the merged `:set` values and the
   non-overlap parts retain the original values. This is the
   surgical equivalent of `DELETE FOR PORTION OF VALID_TIME` followed
   by inserting the per-row updated slice.

   Open-window UPDATE (no `:valid-to`) is not the SQL:2011 semantic
   and is intentionally not supported here — use `upsert!` for that
   shape."
  [ds {:keys [where set] :as _opts} tx-meta]
  (let [cfg (bitemporal-config ds)
        valid (:valid cfg)
        _ (when-not valid
            (throw (ex-info "bounded-update! requires a :valid axis on the dataset"
                            {:bitemporal-config cfg})))
        _ (when (nil? where)
            (throw (ex-info "bounded-update! requires :where" {})))
        _ (when (nil? (:valid-to tx-meta))
            (throw (ex-info "bounded-update! requires tx-meta :valid-to (bounded-window only)"
                            {:tx-meta tx-meta})))
        {vf-col :from-col vt-col :to-col vt-unit :unit} valid
        new-vf (coerce-temporal-value (:valid-from tx-meta) vt-unit)
        new-vt (coerce-temporal-value (:valid-to tx-meta) vt-unit)
        _ (validate-period! (long new-vf) (long new-vt) :valid)
        cols-snap (columns ds)
        n (row-count ds)
        ;; Step 1: capture matching rows that overlap [new-vf, new-vt).
        ;; We materialize each row so we have its values AFTER retract!
        ;; shifts indices in step 2.
        overlapping
        (vec
          (for [i (range n)
                :let [row (materialize-row cols-snap i)]
                :when (let [pred-ok? (eval-pred where row)
                            row-vf  (long (get row vf-col))
                            row-vt  (long (get row vt-col))]
                        (and pred-ok?
                             (not (or (<= row-vt new-vf) (>= row-vf new-vt)))))]
            (let [row-vf (long (get row vf-col))
                  row-vt (long (get row vt-col))]
              {:row row
               :slice-vf (max row-vf new-vf)
               :slice-vt (min row-vt new-vt)})))]
    ;; Step 2: retract over [new-vf, new-vt). The bounded retract
    ;; handles every overlap class — drop / truncate-vt / truncate-vf
    ;; / split — preserving the non-overlap parts of each matched row.
    (retract! ds {:where where} tx-meta)
    ;; Step 3: assert the updated slice per captured row.
    (doseq [{:keys [row slice-vf slice-vt]} overlapping]
      (let [merged (-> row
                       (merge set)
                       (assoc vf-col slice-vf vt-col slice-vt)
                       (dissoc (get-in cfg [:system :from-col])
                               (get-in cfg [:system :to-col])))]
        (append! ds merged tx-meta)))
    ds))

;; ============================================================================
;; Constructor
;; ============================================================================

(defn make-dataset
  "Create StratumDataset from column map.

   Args:
     col-map: {col-name data} where data can be:
              - long[] or double[] (raw arrays)
              - String[] (auto dict-encoded)
              - PersistentColumnIndex (zero-copy)
              - {:type :int64|:float64 :data array} (pre-encoded)

     opts: {:name \"table-name\"       ; Dataset name (default: \"unnamed\")
            :metadata {...}}           ; User metadata map

   When `metadata` contains
     `:bitemporal {:valid {:from-col <kw> :to-col <kw> :unit U}
                   :system {:from-col <kw> :to-col <kw> :unit U}}`
   the configured axis' two columns are stamped with `:temporal-unit U`
   (default `:micros`). Either axis is optional. The config round-trips
   through `sync!`/`load`. See `bitemporal-config` (and the per-axis
   `valid-time-config` / `system-time-config`) for read-back helpers.

   Returns: StratumDataset with normalized columns

   Example:
     (make-dataset {:price (double-array [1.0 2.0 3.0])
                    :qty (long-array [10 20 30])}
                   {:name \"trades\"
                    :metadata {:source \"csv:data.csv\"}})"
  ([col-map] (make-dataset col-map {}))
  ([col-map {:keys [name metadata] :or {name "unnamed" metadata {}}}]
   (when (empty? col-map)
     (throw (ex-info "Cannot create dataset from empty column map"
                     {:col-map col-map})))

   ;; Normalize all columns via encode-column
   ;; This handles: arrays, indices, pre-encoded maps, string dict-encoding
   (let [cols (into {}
                    (map (fn [[k v]]
                           (let [col-name (if (keyword? k) k (keyword k))]
                             [col-name (column/encode-column v)])))
                    col-map)

         ;; Apply :bitemporal metadata config: stamp :temporal-unit on the
         ;; window columns of every configured axis (:valid, :system), and
         ;; surface validation errors early (missing col / wrong type).
         cols (apply-bitemporal-config cols metadata)

         ;; Infer schema from normalized columns
         sch (infer-schema cols)

         ;; Extract row count from first column
         ;; Handle both array-backed and index-backed columns
         first-col (first (vals cols))
         rc (if (:data first-col)
              ;; Array-backed: get array length
              (let [arr (:data first-col)]
                (if (instance? (Class/forName "[D") arr)
                  (alength ^doubles arr)
                  (alength ^longs arr)))
              ;; Index-backed: get index length
              (idx/idx-length (:index first-col)))

         ;; Validate all columns have same length
         _ (doseq [[col-name col-data] cols]
             (let [col-len (if (:data col-data)
                             (let [arr (:data col-data)]
                               (if (instance? (Class/forName "[D") arr)
                                 (alength ^doubles arr)
                                 (alength ^longs arr)))
                             (idx/idx-length (:index col-data)))]
               (when-not (= col-len rc)
                 (throw (ex-info "All columns must have same length"
                                 {:expected rc
                                  :column col-name
                                  :length col-len})))))]

     ;; Create dataset with metadata that includes :name
     (with-meta
       (StratumDataset. name cols sch (long rc) metadata nil nil nil)
       {:name name}))))

;; ============================================================================
;; Column Operations (Explicit API)
;; ============================================================================

(defn add-column
  "Add a column to dataset. Returns new dataset.

   This is an explicit alternative to (assoc ds col-name data).
   Auto-converts data to index via encode-column.
   Validates length matches dataset row count.

   Args:
     ds - StratumDataset (must be persistent, not transient)
     col-name - Keyword column name
     data - Column data: PersistentColumnIndex, array, sequence, or pre-encoded map

   Returns: New StratumDataset with added column

   Example:
     (add-column ds :new-col [1 2 3])
     (add-column ds :price (idx/index-from-seq :float64 [10.0 20.0 30.0]))"
  [ds col-name data]
  (assoc ds col-name data))

(defn drop-column
  "Remove a column from dataset. Returns new dataset.

   This is an explicit alternative to (dissoc ds col-name).

   Args:
     ds - StratumDataset (must be persistent, not transient)
     col-name - Keyword column name to remove

   Returns: New StratumDataset without the column

   Example:
     (drop-column ds :old-col)"
  [ds col-name]
  (dissoc ds col-name))

(defn rename-column
  "Rename a column. Returns new dataset.

   Args:
     ds - StratumDataset (must be persistent, not transient)
     old-name - Keyword current column name
     new-name - Keyword new column name

   Returns: New StratumDataset with renamed column

   Example:
     (rename-column ds :old-name :new-name)"
  [ds old-name new-name]
  (if-let [col-data (get (columns ds) old-name)]
    (-> ds
        (dissoc old-name)
        (assoc new-name col-data))
    ds))  ;; Return unchanged if column doesn't exist

;; ============================================================================
;; Column Conversion
;; ============================================================================

(defn ensure-indexed
  "Convert all array-backed columns to index-backed PersistentColumnIndex.
   Index-backed columns pass through unchanged. Required before sync!
   when dataset was created from arrays (e.g., tuples->columns output)."
  [ds]
  (let [cols (columns ds)
        needs-conversion? (some (fn [[_k v]] (not= :index (:source v))) cols)]
    (if-not needs-conversion?
      ds ;; All columns already index-backed
      (let [converted-columns
            (into {}
                  (map (fn [[col-name col-data]]
                         (if (= :index (:source col-data))
                           [col-name col-data]
                           ;; Array-backed → create index
                           (let [{:keys [type data dict dict-type]} col-data
                                 arr data
                                 n (if (instance? (Class/forName "[D") arr)
                                     (alength ^doubles arr)
                                     (alength ^longs arr))
                                 new-idx (idx/index-from-seq type
                                                             (if (= type :float64)
                                                               (seq ^doubles arr)
                                                               (seq ^longs arr)))
                                 col-entry (cond-> {:type type :source :index :index new-idx}
                                             dict (assoc :dict dict)
                                             dict-type (assoc :dict-type dict-type))]
                             [col-name col-entry]))))
                  cols)
            commit-info (:commit-info ds)]
        (with-meta
          (StratumDataset. (ds-name ds) converted-columns (schema ds)
                           (long (row-count ds)) (metadata ds)
                           nil commit-info (meta ds))
          (meta ds))))))

;; ============================================================================
;; Standalone Persistence Functions
;; ============================================================================

(defn load
  "Load dataset from storage by branch name or commit UUID.
   Returns StratumDataset with all index columns restored."
  [store branch-or-commit]
  (let [;; Resolve to commit ID
        commit-id (if (uuid? branch-or-commit)
                    branch-or-commit
                    (storage/load-dataset-head store branch-or-commit))
        _ (when-not commit-id
            (throw (ex-info "Dataset branch or commit not found"
                            {:branch-or-commit branch-or-commit})))
        ;; Load dataset snapshot
        ds-snapshot (storage/load-dataset-commit store commit-id)
        _ (when-not ds-snapshot
            (throw (ex-info "Dataset commit not found"
                            {:commit-id commit-id})))
        ;; Restore each column's index
        {:keys [name columns schema row-count metadata]} ds-snapshot
        restored-columns
        (into {}
              (map (fn [[col-name col-info]]
                     (let [idx-commit-id (:index-commit col-info)
                           idx-snapshot (storage/load-index-commit store idx-commit-id)
                           _ (when-not idx-snapshot
                               (throw (ex-info "Index commit not found"
                                               {:column col-name
                                                :index-commit idx-commit-id})))
                           restored-idx (idx/restore-index-from-snapshot idx-snapshot store)]
                       [col-name (cond-> {:type (:type col-info)
                                          :source :index
                                          :index restored-idx}
                                   ;; Restore dict for string-encoded columns
                                   (:dict col-info)
                                   (assoc :dict (into-array String (:dict col-info))
                                          :dict-type (:dict-type col-info)))]))
                   columns))
        ;; Re-stamp :temporal-unit on the configured axes' window
        ;; columns from the round-tripped metadata. The per-column
        ;; commit payload does not persist :temporal-unit, so the
        ;; source of truth is :metadata. Same logic as make-dataset,
        ;; applied on restore.
        restored-columns (apply-bitemporal-config restored-columns metadata)
        commit-info {:id commit-id :branch (when-not (uuid? branch-or-commit) branch-or-commit)}]

    (with-meta
      (StratumDataset. name restored-columns schema (long row-count) metadata
                       nil commit-info nil)
      {:name name})))

(defn delete-branch!
  "Delete a dataset branch. Does not delete data (use gc! for that)."
  [store branch]
  (storage/unregister-dataset-branch! store branch))

;; ============================================================================
;; Temporal Resolution
;; ============================================================================

(defn resolve
  "Resolve a temporal reference to a StratumDataset.

   store - konserve store
   ref   - branch name (string) or dataset name
   opts  - {:as-of commit-uuid}      - specific commit
           {:branch \"feature\"}     - branch HEAD
           {:as-of-tx 42}           - Datahike tx floor lookup
           {}                        - default: use ref as branch name

   Resolution order:
   1. :as-of (UUID) -> load store uuid
   2. :as-of-tx (int) -> walk commits on branch, find matching metadata
   3. :branch (string) -> load store branch
   4. Default -> load store ref"
  [store ref opts]
  (cond
    ;; Specific commit UUID
    (:as-of opts)
    (load store (:as-of opts))

    ;; Datahike tx floor lookup
    (:as-of-tx opts)
    (let [tx-id (:as-of-tx opts)
          branch (or (:branch opts) ref)
          commit-id (storage/find-commit-by-metadata
                     store branch
                     (fn [m]
                       (when-let [tx (get m "datahike/tx")]
                         (<= (long tx) (long tx-id)))))]
      (if commit-id
        (load store commit-id)
        (throw (ex-info "No commit found with datahike/tx <= given value"
                        {:branch branch :as-of-tx tx-id}))))

    ;; Branch name
    (:branch opts)
    (load store (:branch opts))

    ;; Default: use ref as branch name
    :else
    (load store ref)))

(defn with-metadata
  "Return a new dataset with updated metadata map.
   Merges meta-map into existing metadata. Preserves commit-info for
   parent chain continuity. Use for adding per-commit info like Datahike
   tx IDs before sync!.

   Example:
     (-> ds
         (with-metadata {\"datahike/tx\" 42})
         (sync! store \"main\"))"
  [ds meta-map]
  (let [current-meta (metadata ds)
        merged-meta (merge current-meta meta-map)
        commit-info (:commit-info ds)]
    (StratumDataset. (ds-name ds) (columns ds) (schema ds)
                     (long (row-count ds)) merged-meta
                     nil commit-info (meta ds))))

(defn with-parent
  "Return a new dataset with parent commit-info from another dataset.
   This establishes the parent chain so sync! records the parent
   commit, enabling history traversal and time-travel."
  [ds parent-ds]
  (let [ci (:commit-info parent-ds)]
    (StratumDataset. (ds-name ds) (columns ds) (schema ds)
                     (long (row-count ds)) (metadata ds)
                     nil ci (meta ds))))

(comment
  ;; ============================================================================
  ;; Usage Examples
  ;; ============================================================================

  ;; Create from arrays (query-only, no persistence)
  (def ds (make-dataset {:price (double-array [100.0 200.0 300.0])
                         :qty (long-array [10 20 30])}
                        {:name "trades"}))

  (ds-name ds)          ;; => "trades"
  (row-count ds)        ;; => 3
  (column-names ds)     ;; => (:price :qty)
  (schema ds)           ;; => {:price {:type :float64 :nullable? true}
                        ;;     :qty {:type :int64 :nullable? true}}

  ;; Create from indices (supports persistence)
  (require '[stratum.index :as idx])
  (def price-idx (idx/index-from-seq :float64 [100.0 200.0 300.0]))
  (def qty-idx (idx/index-from-seq :int64 [10 20 30]))
  (def ds2 (make-dataset {:price price-idx :qty qty-idx}
                         {:name "trades-indexed"}))

  (has-index? ds2 :price)  ;; => true
  (chunk-stats ds2 :price) ;; => [ChunkStats ...]

  ;; Fork → Transient → Modify → Persistent
  (def ds3 (fork ds2))
  (def ds3t (transient ds3))
  (set-at! ds3t :price 0 999.0)
  (persistent! ds3t)

  ;; Sync/Load round-trip
  (require '[konserve.memory :refer [new-mem-store]])
  (def store (new-mem-store (atom {}) {:sync? true}))
  (def saved (sync! ds3 store "main"))
  (def loaded (load store "main")))
