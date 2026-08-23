# GC and store ownership

Stratum's garbage collection deliberately differs from datahike's, scriptum's
and proximum's. This document explains why the difference is structural rather
than an oversight, what invariant keeps it safe, and what would have to be
decided to unify the ecosystem — a decision that is still open, and recorded
here so it is made deliberately rather than by drift.

## Two patterns exist in the ecosystem

| adapter | where its data lives | `mark-from-key-map` |
| --- | --- | --- |
| stratum | datahike's store (shared) | real — contributes its reachable keys |
| proximum | its own store (`:store-config`) | `#{}` |
| scriptum | its own path (`<store>-ft` sidecar) | `#{}` |

## Consistency splits into two axes

**Axis A — the collection mechanism.** Guard the values-then-pointer window,
derive the sweep cutoff from `konserve.gc-guard/cutoff` *before* marking, sweep
with `konserve.gc/sweep!`.

- datahike: yes
- scriptum: yes (as of the konserve backing)
- proximum: yes (as of `feat/konserve-gc-guard`)
- stratum: no — a per-store JVM monitor, no timestamp cutoff, deletion limited
  to three explicitly enumerated key families

**Axis B — store ownership.** Own your store and sweep it (scriptum, proximum)
versus share a store and contribute marks (stratum).

## Stratum's position on A follows from its position on B

`konserve.gc/sweep!` is allow-list: it deletes every key that is not in the
whitelist and is older than the cutoff. That is only safe for a store you own
outright.

Stratum shares datahike's store — stated in
`datahike/index/secondary/stratum.clj`:

> Stratum shares datahike's store […] stratum writes to datahike's konserve
> store, so datahike's GC must preserve stratum's keys.

So stratum's explicit-family deletion is not an oversight; it is the adaptation
that sharing forces. Porting it to `sweep!` would require a whitelist covering
every key datahike owns, in the one configuration where getting that wrong
destroys another library's data.

Stratum is already safe on both of its paths:

- **Embedded**: datahike opens `guard/writing!` *before* the secondary flush —
  `datahike/writing.cljc` names stratum in the comment, because `-sec-flush`
  writes konserve keys from inside it — and absorbs stratum's marks via
  `sec/mark-from-key-map :stratum`.
- **Standalone**: `with-storage-lock` serializes `gc!` against `sync!`.

## The decision is B, not A

**Model 1 — every library owns its store.** Stratum moves out of datahike's
store, adopts A wholesale, and its `mark-from-key-map` becomes `#{}` like the
others.

- Buys: one mechanism everywhere; no cross-library mark protocol; a mark bug can
  only damage its own data.
- Costs: N stores to configure, back up and replicate; no structural sharing
  across libraries; a migration for stratum.

**Model 2 — one store, many marks.** Scriptum and proximum move into datahike's
store and implement real marks.

- Buys: one store to operate; one GC; secondary data co-located with the datoms
  that reference it.
- Costs: every `mark-from-key-map` must be exhaustively correct or datahike's
  sweep deletes that library's data. One library's mark bug becomes another's
  data loss.

Two prerequisites for Model 2 exist today. Scriptum's konserve backing makes
it *possible* for scriptum at all — under the earlier path design its data was
files, which is why its `-sec-mark` still reads "Scriptum uses filesystem, not
konserve — nothing to mark". And `konserve.gc-guard` makes it *safe*, being
the shared safe point across libraries in one store.

Weak recommendation: **Model 1.** Failure containment is worth more than
operational tidiness, and landing this work surfaced two independent mark bugs —
proximum's mark never whitelisted branch heads, so every GC deleted the head
commit of every branch. Model 2 asks you to bet against exactly that class of
bug.

## Tripwire

If scriptum's konserve backing ever shares datahike's store,
`mark-from-key-map :scriptum` returning `#{}` becomes a data-loss bug. It is
correct today only because scriptum owns its store.

## The GC/sync lock key (a fixed bug, kept as a design rule)

`with-storage-lock` allocated its monitor in a map keyed by the store
*reference*, reasoning that record equality would make two connections to one
path share a lock. Measured, they do not:

```
identical? false    = false    same hash? false
```

So a process that connected twice got two independent monitors and no exclusion
at all — 0.16 ms of contention where the lock should have blocked for 300 ms.

The accompanying claim that "under-locking is impossible: distinct keys always
get distinct locks" was backwards. Two distinct keys for one physical store *is*
under-locking. The safe direction is the opposite, and it is the same rule the
GC safe point follows: the key may be **coarser** than the physical store — two
stores collapsing onto one monitor merely over-locks — but never **finer**.

The lock now keys on konserve's `:id`, which is coarser (a logical identity,
shared by replicas across machines) and within one JVM names one store. Stores
built through a backend constructor carry no id and fall back to the object,
which is the old behaviour.
