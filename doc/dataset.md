# Dataset and Persistence

This document covers Stratum's dataset type, persistence model, and temporal query capabilities.

## StratumDataset

`StratumDataset` is the primary table abstraction in Stratum. It wraps a collection of typed columns with schema, metadata, and persistence support.

### Creating Datasets

```clojure
(require '[stratum.api :as st]
         '[stratum.dataset :as dataset]
         '[stratum.index :as idx])

;; From raw arrays (query-only, no persistence)
(def ds (st/make-dataset
          {:price (double-array [10.0 20.0 30.0])
           :qty   (long-array [1 2 3])}
          {:name "trades"}))

;; From indices (supports persistence, zone maps, O(1) fork)
(def ds (st/make-dataset
          {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])
           :qty   (idx/index-from-seq :int64 [1 2 3])}
          {:name "trades"}))
```

Options for `make-dataset`:
- `:name` - Dataset name string (default: `"unnamed"`)
- `:metadata` - User metadata map (stored in commits). A reserved key
  `:bitemporal {:valid {...} :system {...}}` opts the dataset into
  bitemporal valid-time / system-time semantics — see
  [Bitemporal Windows](#bitemporal-windows).

### Column Types

| Input Type | Internal Format | Persistence | Zone Maps |
|-----------|----------------|-------------|-----------|
| `long[]` | `{:type :int64 :data long[]}` | No | No |
| `double[]` | `{:type :float64 :data double[]}` | No | No |
| `String[]` | Dict-encoded `{:type :int64 :data long[] :dict String[]}` | No | No |
| `PersistentColumnIndex` | `{:type T :source :index :index idx}` | Yes | Yes |

Only index-backed columns support persistence (`st/sync!`) and O(1) forking (`st/fork`).

### Accessors

```clojure
(st/name ds)          ;; => "trades"
(st/row-count ds)     ;; => 3
(st/column-names ds)  ;; => (:price :qty)
(st/schema ds)        ;; => {:price {:type :float64 :nullable? true} ...}
(st/columns ds)       ;; => normalized column map for query engine
```

### Column Operations

```clojure
;; Add column (returns new dataset, validates length)
(assoc ds :revenue (double-array [100.0 200.0 300.0]))

;; Remove column
(dissoc ds :old-col)

;; Rename column
(dataset/ds-rename-column ds :old-name :new-name)
```

## Transient/Persistent Lifecycle

Like Clojure collections, mutations require transient mode:

```clojure
;; CORRECT - transient → mutate → persistent
(-> ds
    dataset/ds-transient
    (dataset/ds-set! :price 0 99.0)
    (dataset/ds-append! {:price 40.0 :qty 4})
    dataset/ds-persistent!)

;; WRONG - will throw IllegalStateException
(dataset/ds-set! ds :price 0 99.0)
```

Only index-backed datasets support transient mode.

## Fork Semantics

Forking is O(1) via structural sharing:

```clojure
(def fork (st/fork ds))
;; fork shares all data with ds
;; mutations to fork's transient only copy affected chunks (CoW)
```

## Persistence

### Sync and Load

```clojure
(require '[konserve.store :as kstore])

;; Create store
(def store (kstore/create-store {:backend :file :path "/tmp/stratum-data"
                                  :id (java.util.UUID/randomUUID)}
                                 {:sync? true}))

;; Save to branch
(def saved (st/sync! ds store "main"))

;; Load from branch HEAD
(def loaded (st/load store "main"))

;; Load from specific commit
(def at-commit (st/load store commit-uuid))
```

### Branch Management

```clojure
;; List branches
(stratum.storage/list-dataset-branches store)
;; => #{"main" "feature-1"}

;; Delete branch (data reclaimed by GC)
(dataset/ds-delete-branch! store "feature-1")

;; Garbage collect unreachable data
(st/gc! store)
```

### Storage Layout

Stratum uses konserve with the following key structure:

| Key | Contents |
|-----|----------|
| `[:datasets :branches]` | Set of branch names |
| `[:datasets :heads "main"]` | Branch HEAD commit UUID |
| `[:datasets :commits <uuid>]` | Dataset snapshot (columns, schema, metadata) |
| `[:indices :commits <uuid>]` | Index snapshot (PSS root, chunk size, stats) |
| `<uuid>` | PSS tree node (Leaf or Branch) |

### Commit Metadata

Commits store user metadata from the dataset. Use `st/with-metadata` to add per-commit info:

```clojure
(-> ds
    (st/with-metadata {"datahike/tx" 42
                          "synced-at" (System/currentTimeMillis)})
    (st/sync! store "main"))
```

## Temporal Queries

Stratum supports querying datasets at specific points in time.

### By Commit UUID

```clojure
(st/q {:from "trades" :agg [[:sum :price]]}
     {:store store :as-of commit-uuid})
```

### By Branch

```clojure
(st/q {:from "trades" :agg [[:sum :price]]}
     {:store store :branch "main"})
```

### By Datahike Transaction (Floor Lookup)

When syncing with Datahike tx metadata, query at a specific transaction point:

```clojure
(st/q {:from "trades" :agg [[:sum :price]]}
     {:store store :as-of-tx 42})
```

This walks the commit history and finds the most recent commit with `"datahike/tx"` metadata `<= 42`.

### Direct Resolution

```clojure
;; Resolve without querying
(def ds-at-time (st/resolve store "trades" {:as-of commit-uuid}))
(def ds-at-tx (st/resolve store "trades" {:as-of-tx 42}))
```

### Bitemporal Windows

The temporal axes above are **tx-time** (when something was *committed* to this dataset). Many domains — accounting, contracts, regulatory reporting — also need **valid-time** (when a fact was *true in the modelled world*) and sometimes **system-time** (when the DB *recorded* a fact, per-row, distinct from per-commit). Stratum supports both as opt-in column conventions on `make-dataset`. See [`temporal-design.md`](temporal-design.md) for the full design.

```clojure
(st/make-dataset
  {:eid          (long-array [1 1 2])
   :salary       (long-array [100000 110000 80000])
   :_valid_from  (long-array [1704067200000000  ;; 2024-01-01 μs
                              1719792000000000  ;; 2024-07-01 μs
                              1709251200000000]);; 2024-03-01 μs
   :_valid_to    (long-array [1719792000000000
                              Long/MAX_VALUE
                              Long/MAX_VALUE])
   :_system_from (long-array [1704067200000000
                              1719792000000000
                              1709251200000000])
   :_system_to   (long-array [Long/MAX_VALUE
                              Long/MAX_VALUE
                              Long/MAX_VALUE])}
  {:name "salaries"
   :metadata
   {:bitemporal
    {:valid  {:from-col :_valid_from
              :to-col   :_valid_to
              :unit     :micros}
     :system {:from-col :_system_from
              :to-col   :_system_to
              :unit     :micros}}}})
```

Either axis is optional — a dataset can opt into only `:valid` (most common), only `:system`, or both. Each axis is processed identically:

- both named columns exist,
- both are `:int64`,
- no conflicting pre-existing `:temporal-unit` is set,

and `make-dataset` stamps each pair with `:temporal-unit :micros` (the default — overridable to `:days`, `:seconds`, `:millis`). The columns are queryable like any other temporal column, including `DATE_TRUNC` / `EXTRACT` and zone-map range pruning. The `:micros` default matches the DuckDB `TIMESTAMP` convention used by the SQL surface and XTDB v2's per-row temporal columns.

Read the config back via `stratum.dataset/bitemporal-config`:

```clojure
(dataset/bitemporal-config ds)
;; => {:valid  {:from-col :_valid_from  :to-col :_valid_to  :unit :micros}
;;     :system {:from-col :_system_from :to-col :_system_to :unit :micros}}

(dataset/valid-time-config ds)
;; => {:from-col :_valid_from :to-col :_valid_to :unit :micros}

(dataset/system-time-config ds)
;; => {:from-col :_system_from :to-col :_system_to :unit :micros}
```

The metadata round-trips through `sync!` / `load`; the per-column `:temporal-unit` tag is re-applied automatically on restore (metadata is the source of truth, since per-column commit payloads don't persist `:temporal-unit`).

Half-open `[from, to)` windows are the canonical encoding. `Long/MAX_VALUE` is the open-ended sentinel; zone-map pruning handles it without special-casing.

Typical as-of-vt query (one entity's salary on a given date):

```clojure
(st/q {:from "salaries"
       :where [[:= :eid 1]
               [:<= :_valid_from at-micros]
               [:>  :_valid_to   at-micros]]
       :select [:salary]})
```

For per-row SCD2 (close the previous window when an entity is updated), use the `vt-append!` / `vt-update!` / `vt-delete!` primitives — see [`temporal-design.md`](temporal-design.md) for the API and overlap-policy.

The `bench/baseline-vt-branch.txt` snapshot captures three valid-time bench shapes — point-in-vt at 1% selectivity, 50% selectivity with the `MAX_VALUE` sentinel, and group-by with vt filter — alongside the standard tier 1-9 set, so regressions on the underlying scan + zone-map paths are caught against the same reference.

## Integration with Query Engine

Datasets work directly as `:from` sources:

```clojure
;; Dataset as :from (preferred)
(st/q {:from my-dataset
      :where [[:> :price 100]]
      :agg [[:sum :price] [:count]]
      :group [:region]})

;; Index-backed datasets get zone map pruning automatically
```

The query engine extracts normalized columns via `st/columns` and routes to the optimal execution strategy.
