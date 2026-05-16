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
  `:valid-time {:from-col <kw> :to-col <kw> :unit :micros}` opts the
  dataset into bitemporal valid-time queries — see [Valid-Time Window](#valid-time-window).

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

### Valid-Time Window

The temporal axes above are **transaction-time** (the time something was committed). Many domains — accounting, contracts, regulatory reporting — also need **valid-time** (when a fact was true in the modelled world), which can be very different from the commit time. Stratum supports valid-time as an opt-in column convention on `make-dataset`:

```clojure
(st/make-dataset
  {:eid         (long-array [1 1 2])
   :salary      (long-array [100000 110000 80000])
   :_valid_from (long-array [1704067200000000  ;; 2024-01-01 μs
                             1719792000000000  ;; 2024-07-01 μs
                             1709251200000000]);; 2024-03-01 μs
   :_valid_to   (long-array [1719792000000000
                             Long/MAX_VALUE
                             Long/MAX_VALUE])}
  {:name "salaries"
   :metadata {:valid-time {:from-col :_valid_from
                           :to-col   :_valid_to
                           :unit     :micros}}})
```

When `:metadata` carries `:valid-time {:from-col <kw> :to-col <kw>}`, `make-dataset` validates that:

- both named columns exist,
- both are `:int64`,
- no conflicting pre-existing `:temporal-unit` is set,

and stamps each with `:temporal-unit :micros` (the default — overridable to `:days`, `:seconds`, `:millis`). The two columns are then queryable like any other temporal column, including `DATE_TRUNC` / `EXTRACT` and zone-map range pruning. `:unit` defaults to `:micros` to match the DuckDB `TIMESTAMP` convention used by the SQL surface.

Read the config back via `stratum.dataset/vt-config`:

```clojure
(dataset/vt-config ds)
;; => {:from-col :_valid_from :to-col :_valid_to :unit :micros}
```

The metadata round-trips through `sync!` / `load`; the per-column `:temporal-unit` tag is re-applied automatically on restore (the metadata is the source of truth, since the per-column commit payload doesn't carry it).

Half-open `[from, to)` windows are the recommended encoding. `Long/MAX_VALUE` is the conventional open-ended sentinel; zone-map pruning handles it without special-casing.

Typical as-of-vt query (one entity's salary on a given date):

```clojure
(st/q {:from "salaries"
       :where [[:= :eid 1]
               [:<= :_valid_from at-micros]
               [:>  :_valid_to   at-micros]]
       :select [:salary]})
```

For per-row SCD2 (close the previous window when an entity is updated), see `datahike-bitemporal-v1`'s `stratum.datahike` adapter, which uses this convention as its on-disk row layout.

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
