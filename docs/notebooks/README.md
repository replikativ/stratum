# Stratum Notebooks

This directory contains interactive notebooks showcasing Stratum's features.

## Available Notebooks

- **stratum_intro.clj** — Comprehensive introduction to Stratum covering:
  - Query engine basics
  - Tablecloth/tech.ml.dataset integration
  - Performance and SIMD execution
  - Persistent indices with copy-on-write
  - Zone map pruning
  - Statistical aggregations
  - Hash joins
  - SQL interface
  - Real-world examples

## Using Notebooks

### In the REPL

Start a REPL with notebook support:

```bash
clj -M:repl
```

Then load and evaluate the notebook:

```clojure
(load-file "notebooks/stratum_intro.clj")
```

### With Clay

Stratum notebooks use the [Kindly](https://scicloj.github.io/kindly/) convention and can be rendered with [Clay](https://scicloj.github.io/clay/):

```clojure
(require '[scicloj.clay.v2.api :as clay])

;; Render to HTML
(clay/make! {:source-path "notebooks/stratum_intro.clj"
             :show true})  ;; Opens in browser

;; Or generate Quarto document
(clay/make! {:source-path "notebooks/stratum_intro.clj"
             :format [:quarto :html]})
```

### Evaluating Sections

The notebooks are regular Clojure files with comment-based documentation. You can:

1. Load the entire file in your REPL
2. Evaluate sections one at a time using your editor's eval commands
3. Copy-paste examples into your own code

## Creating New Notebooks

Follow the Kindly convention:

1. Use `;;` for documentation (rendered as markdown)
2. Use `;; #` for headers, `;; ##` for subheaders
3. Code blocks are regular Clojure expressions
4. Use `scicloj.kindly.v4.kind` namespace for special rendering hints

Example:

```clojure
;; # My Notebook Title

;; This is regular text that explains something.

(ns my-notebook
  (:require [stratum.api :as stratum]
            [scicloj.kindly.v4.kind :as kind]))

;; ## Section Header

;; More explanation here.

(def my-data {:x [1 2 3]})

;; The result will be displayed:
my-data
```

## Dependencies

Notebooks have access to:
- All Stratum namespaces
- `tablecloth.api` and `tech.v3.dataset` for dataframe interop
- `scicloj.kindly.v4.kind` for rendering hints (when using Clay)

## Learn More

- [Kindly documentation](https://scicloj.github.io/kindly/)
- [Clay documentation](https://scicloj.github.io/clay/)
- [Noj notebooks](https://scicloj.github.io/noj/) for more examples
