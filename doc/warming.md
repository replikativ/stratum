# Warming

Pull a dataset's index trees into the storage cache in concurrent waves instead
of discovering them one blocking round trip at a time.

## The problem it solves

A dataset loaded from storage (`dataset/load`) is **lazy**: its column trees
come back address-rooted, and every node a query touches is a storage round
trip, paid one at a time — a scan asks for a node, blocks on the read, and only
then learns the next address. On a local filestore that is noise. Against an
object store it is the whole cost: a cold reader's wall time is
`misses × RTT` with nothing overlapping.

The fix needs no prediction. A tree branch holds **every child address the
moment it is materialized**, so a whole level's addresses are known one level
in advance. `stratum.warm` walks breadth-first and fetches each level
concurrently — the walk itself lives in
[persistent-sorted-set](https://github.com/replikativ/persistent-sorted-set)
(`org.replikativ.persistent-sorted-set.warm`); this namespace is the
dataset-shaped entry.

## Use

```clojure
(require '[stratum.warm :as warm])

(def ds (dataset/load store "main"))

;; connect-time: warm every indexed column's spine, one shared budget
(warm/warm! ds)                                   ; {:depth :interior}

;; before a scan you know is coming: materialize the column data
(warm/warm! ds {:depth :with-leaves :budget 2000})

;; one column only
(warm/warm-column! ds :price {:depth :with-leaves})
```

All indexed columns share **one budget round-robin** — no column can spend it
before another gets any, because which column the next query needs is not
knowable at warm time. The report's `:by-index` says where the budget actually
went, keyed by column name.

## Depth: a bigger lever here than in a thin-leaf tree

A stratum leaf is **fat**: each `ChunkEntry` in it carries its whole
`PersistentColChunk`, so one leaf is up to 64 chunks of column data (~4 MB at
the default chunk size) and restoring the leaves *is* materializing the column.

| `:depth` | warms | for |
|---|---|---|
| `:interior` (default) | the spine — branches carry the `ChunkStats` measures (min/max/sum/count) | stats, measure-driven pruning and chunk addressing answer without touching a chunk |
| `:with-leaves` | the column data itself | a scan you know is coming; size `:budget` to what it will read |
| integer | at most that many levels below the root | fine control |

`:budget` bounds the cost (nodes fetched, hard ceiling), `:depth` bounds the
shape, and whichever binds first wins — a small dataset with a large budget
runs out of tree and has fetched itself entirely; a large one hits the budget
and stops. Same code, no modes, no latency cliffs.

## The report

```clojure
{:fetched 63 :by-level [7 30 26] :rounds 3 :height 3
 :by-index {:price 31 :qty 32}
 :budget-left 937 :budget-exhausted? false :budget-clamped? false :ms 12.4}
```

`:by-level` and `:budget-exhausted?` are the point: they make a decaying warm
visible as a metric before it is visible in p99. A warm that starts reporting
`:budget-exhausted? true` on a growing dataset is telling you to raise the
budget or accept partial warmth — before your users tell you.

## What warming does not promise

- **It changes no results.** A warm only moves nodes into the cache earlier and
  more concurrently than a scan would; skipping it, or running out of budget,
  costs round trips and never correctness.
- **`:fetched` counts restores issued, not cache misses.** Nodes already
  resident (from `dataset/load` itself, or a previous warm) are counted by the
  walk but cost no storage read.
- **Blob granularity is stratum's, not the tree's.** Stratum can persist a
  small tree as one or two storage blobs, so read counts do not map one-to-one
  onto nodes. The contract that holds — and that the test suite asserts — is:
  after a full warm the scan that follows performs **zero** further storage
  reads, the budget is an exact ceiling, and warming never makes a scan more
  expensive.

## Where this sits in the stack

The same walk warms datahike's primary indices, and stratum's per-attribute
trees when stratum runs as a datahike secondary index. Scriptum (full-text)
warms differently — its data is Lucene segment files fetched whole from a
manifest, no tree to walk — and proximum (vector search) loads its graph
eagerly on restore. Each library warms in its own units; what is shared is the
report shape and the discipline: budget-bounded, measured, and honest about
what was left cold.
