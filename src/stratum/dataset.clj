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
    "Set value at row position in a column. Must be transient.")

  (append! [this row-map]
    "Append row values across all columns. Must be transient.
     row-map: {col-kw value}")

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
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent dataset. Call transient first.")))
    (doseq [[col-name col-data] columns-field]
      (let [val (get row-map col-name)]
        (when (nil? val)
          (throw (ex-info "append! requires values for all columns"
                          {:missing col-name :columns (keys columns-field)})))
        (idx/idx-append! (:index col-data) val)))
    (set! row-count-val (unchecked-inc row-count-val))
    this)

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
