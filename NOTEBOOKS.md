# Stratum Notebooks

Interactive notebooks for exploring Stratum's features.

## Quick Start

### Render with Clay

```clojure
;; In a running REPL (clj -M:repl):
(require '[scicloj.clay.v2.api :as clay])

;; Render to HTML - opens in browser at http://localhost:1971/
(clay/make! {:source-path "notebooks/stratum_intro.clj" :show true})

;; Generate Quarto document
(clay/make! {:source-path "notebooks/stratum_intro.clj" :format [:quarto :html]})
```

### Evaluate in REPL

```bash
clj -M:repl
# then:
(load-file "notebooks/stratum_intro.clj")
```

### Run test suite

```bash
clj -M:dev -i notebooks/test_notebook.clj
clj -M:dev -i notebooks/test_persistence.clj
```

## Available Notebooks

### `stratum_intro.clj`

Introduction for Clojure data science practitioners:

- **Column maps & SQL** - DSL query maps and SQL strings, same engine
- **Tablecloth interop** - Pass `tc/dataset` directly, zero copy
- **Fused SIMD execution** - Why it's fast, live timing on 1M rows
- **Zone map pruning** - Range queries skip irrelevant chunks automatically
- **Persistence** - `st/sync!`, `st/fork`, `st/load`, time-travel by commit UUID
- **Statistics** - STDDEV, VARIANCE, CORR natively in a single pass
- **Hash joins** - INNER, LEFT, RIGHT, FULL

### `datahike_integration.clj`

Datahike + Stratum - entity queries alongside OLAP analytics, auto-sync
via `d/listen!`, Yggdrasil composite for atomic snapshots, SQL via PgWire.

## Writing Notebooks

Notebooks are regular Clojure files. Comments become prose, expressions
become output cells when rendered by Clay.

```clojure
;; # My Notebook Title

;; Explanation text here.

(ns my-notebook
  (:require [stratum.api :as st]
            [scicloj.kindly.v4.kind :as kind]))

;; ## Section

(st/q {:from {:x (double-array [1 2 3])} :agg [[:sum :x]]})
```

## Scicloj Ecosystem

Stratum notebooks follow the [Kindly](https://scicloj.github.io/kindly/)
convention and work with [Clay](https://scicloj.github.io/clay/) for
static site generation.
