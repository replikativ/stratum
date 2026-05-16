# Audit and Integrity Verification

Stratum stores configured with `:crypto-hash? true` produce content-addressed
commits whose UUIDs are deterministic hashes of the payload. Any bytes-level
tampering on the underlying konserve blobs surfaces as a recomputed UUID that
no longer matches the address it's stored under. `stratum.audit` exposes the
verification surface that turns this property into actionable reports.

The audit namespace mirrors datahike's `datahike.index.audit` in protocol
shape (`IAuditable`, `-merkle-root`, `-recompute-merkle-root`) and result-map
vocabulary (`:ok`, `:mismatch`, `:unsupported`, `:advisory`, `:incomplete`),
so bridges that compose both engines (e.g. datahike's stratum secondary
index) can pass results through without translation.

## When to enable

Audit verification only produces meaningful results when the store was
created with `:crypto-hash? true`:

```clojure
(st/sync! ds store "main" {:crypto-hash? true})
```

Without crypto-hashing, dataset and index commit UUIDs are random, so the
"does the recomputed cid match the stored cid?" question is meaningless.
`stratum.audit` detects this and returns `{:status :unsupported :reason
:crypto-hash-disabled}` rather than a false positive. `crypto-hash?` is a
per-write decision; you can mix crypto-hashed and random-uuid commits on
the same store, but only the crypto-hashed ones are verifiable.

## `verify-chain`

`stratum.audit/verify-chain` walks the dataset-commit DAG backwards from a
branch HEAD via `:parents`, recomputes each commit-id from its stored
snapshot, and reports anomalies:

```clojure
(require '[stratum.audit :as audit])

(audit/verify-chain store)
;; => {:head        #uuid "..."
;;     :status      :ok            ;; :ok | :mismatch | :advisory | :incomplete
;;     :commits     [{:cid ...     ;; one entry per visited commit
;;                    :recomputed ...
;;                    :parents [...]
;;                    :status :ok|:mismatch|:advisory}
;;                   ...]
;;     :mismatches  []              ;; subset of :commits where :status = :mismatch
;;     :missing     []}             ;; cids referenced as :parents but not loadable
```

Options:

| Key       | Default          | Meaning                                                       |
|-----------|------------------|---------------------------------------------------------------|
| `:branch` | `"main"`         | Branch to read HEAD from (ignored when `:head` is supplied)   |
| `:head`   | branch HEAD      | Explicit commit-id to start the walk from                     |
| `:limit`  | `Long/MAX_VALUE` | Stop after this many commits — useful for large histories     |
| `:deep?`  | `false`          | Also walk every column's PSS tree under the head (see below)  |

This is the *layer-1* check: it catches tampering with the commit metadata
itself (the JSON-ish payload that `gen-address` hashes to produce the cid).
It does **not** load the column data — that's `:deep? true`'s job.

A small convenience:

```clojure
(audit/ok? (audit/verify-chain store))
;; => true / false
```

## `:deep? true` — PSS tree walk

When `:deep? true`, after the chain walk, every column's PSS (persistent
sorted set) tree under the head dataset commit is loaded from konserve and
walked node-by-node. For each node:

1. Read the bytes directly (bypassing any in-memory cache, so a hot cache
   can't mask a tampered blob).
2. Recompute its content-addressed UUID via `stratum.cached-storage/gen-address`.
3. If the recomputed UUID differs from the address the blob is stored at,
   record `{:type :audit/merkle-mismatch :address ... :expected ... :recomputed ...}`.

The result map gains a `:deep` entry summarising per-column status:

```clojure
{:status :ok
 :deep   {:status :ok
          :diffs  []}            ;; empty when clean; populated with column-keyed errors otherwise
 ...}
```

This is the *layer-2* check: it catches bytes-level tampering on the
`.ksv` blobs that the PSS tree actually persists into. `:deep? true` reads
the full tree, so it's the expensive option — use it on a fresh schedule
(daily / weekly / pre-restore), not on every query.

## Live-instance API — `IAuditable`

`StratumDataset` and `PersistentColumnIndex` both implement the
`IAuditable` protocol, which gives you a uniform API for verifying a live
value without going through `verify-chain`:

```clojure
(audit/-merkle-root ds)
;; => #uuid "..."            ;; current commit-id; nil if unsynced

(audit/-recompute-merkle-root ds)
;; => {:status :ok :root #uuid "..."}
;;    or
;;    {:status :mismatch :root nil :errors [...]}
;;    or
;;    {:status :unsupported :reason :unsynced | :no-store | :crypto-hash-disabled}
```

`-merkle-root` is cheap (returns the cached cid). `-recompute-merkle-root`
internally calls `verify-chain` with `:deep? true` against the live value's
store, so it has the same cost profile as a deep chain walk.

The same protocol shape lives in datahike (`datahike.index.audit`), and
bridges like the stratum secondary index in datahike implement `IAuditable`
on their wrapper types so a single audit pass over a datahike connection
covers both layers' state.

## `verify-pss-tree-from-cold` — column-level entry

If you want to verify a single column without walking the whole dataset:

```clojure
(audit/verify-pss-tree-from-cold store pss-root-uuid)
;; => {:status :ok}
;;    or
;;    {:status :mismatch :errors [...]}
```

`pss-root` is the UUID found in an index commit snapshot under `:pss-root`.
The `:verified` option (an atom holding `#{addresses}`) shares dedup state
across calls when verifying multiple columns of one dataset, since stratum
often reuses identical PSS subtrees across commits.

## Result-map vocabulary

| `:status`       | Meaning                                                       |
|-----------------|---------------------------------------------------------------|
| `:ok`           | Everything verified clean                                     |
| `:mismatch`     | At least one cid or node bytes differs from its recomputed UUID|
| `:unsupported`  | Crypto-hashing wasn't enabled (or the value is unsynced)      |
| `:incomplete`   | A `:parents` cid couldn't be loaded — chain truncated         |
| `:advisory`     | Non-fatal issue noted in commit metadata (reserved)           |

A `:mismatch` is the actionable case: a stored blob's bytes no longer hash
back to the address it lives at, which means either the blob was modified
out-of-band or the hash algorithm itself changed (e.g., konserve format
upgrade with insufficient migration). The `:errors` vector pinpoints which
addresses + which node classes are involved.

## See also

- [`doc/storage-and-indices.md`](storage-and-indices.md) — the merkle-mode
  background that `verify-chain` builds on.
- The corresponding datahike namespace `datahike.index.audit` — same
  protocol shape, lets you write a single audit driver that covers both
  the datahike primary indices and a stratum secondary index in one pass.
