# Stratum Notebooks

Literate documentation and tests for Stratum, rendered as a
[Quarto](https://quarto.org/) book via [Clay](https://scicloj.github.io/clay/).

## Structure

```
notebooks/
├── chapters.edn              # book outline — parts and chapter filenames
├── index.clj                 # landing page (renders README.md)
├── dev.clj                   # build helpers (make-book!, make-gfm!)
├── custom.scss               # Quarto styling (Fira Code, table tweaks)
└── stratum_book/             # chapters live here
    ├── quickstart.clj
    ├── columnar_internals.clj
    ├── dataset_persistence.clj
    ├── tablecloth_interop.clj
    └── api_reference.clj

clay.edn                      # Clay config (Quarto theme, target paths)
```

## Requirements

The Quarto CLI needs to be [installed](https://quarto.org/docs/get-started/).

## How it works

Each `.clj` file under `stratum_book/` is a regular Clojure namespace
that doubles as a notebook. Prose goes in `;;` comments (rendered as
Markdown), and top-level expressions become evaluated code cells.

Assertions use `kind/test-last`:

```clojure
(st/q {:from data :agg [[:count]]})

(kind/test-last
 [(fn [result] (= 1 (count result)))])
```

Clay's [test generation](https://scicloj.github.io/clay/clay_book.test_generation.html)
turns these annotations into standard `clojure.test` / `deftest` forms,
so the assertions run as part of the regular test suite.

## Documenting API entries with `kind/doc`

For API reference pages, use
[`kind/doc`](https://scicloj.github.io/clay/#documentation)
to generate a ### heading from a var's docstring. These headings
appear in Quarto's table of contents automatically.

Add metadata to the `ns` form so the `kind/doc` calls themselves
are hidden in the rendered output:

```clojure
^{:kindly/hide-code true
  :kindly/options {:kinds-that-hide-code #{:kind/doc}}}
(ns stratum-book.api-reference
  (:require [stratum.api :as st]
            [scicloj.kindly.v4.kind :as kind]))
```

Then, instead of writing manual `### heading` comments, call
`kind/doc` on the var:

```clojure
(kind/doc #'st/q)

;; Free-form prose and examples follow as usual.

(st/q {:from data :agg [[:sum :qty]]})

(kind/test-last
 [(fn [result] (= 1 (count result)))])
```

Use `## Section` comments (level 2) for grouping entries into
logical sections, and `kind/doc` (which renders as level 3) for
individual API entries within each section.

## Building the book

Start a REPL with the `:dev` alias, then:

```clojure
(require '[dev :refer [make-book! make-gfm!]])

(make-book!)           ; → docs/  (Quarto HTML book)
(make-gfm!)            ; → gfm/   (GitHub-flavored Markdown)
(make-gfm! "stratum_book/quickstart.clj")  ; single chapter
```

`make-book!` reads `chapters.edn` to assemble the book structure,
then calls Clay which:

1. Evaluates each `.clj` file top-to-bottom
2. Converts the output to `.qmd` (Quarto Markdown)
3. Runs Quarto to produce the HTML site in `docs/`

## Adding a chapter

1. Create `notebooks/stratum_book/my_chapter.clj` with an `(ns stratum-book.my-chapter ...)` form
2. Add `"my_chapter"` to the appropriate part in `chapters.edn`
3. Run `(make-book!)` to verify

## Running tests

The notebooks are on the `:test` classpath. `kind/test-last`
annotations [generate](https://scicloj.github.io/clay/clay_book.test_generation.html)
`deftest` forms that the test runner picks up
alongside traditional `deftest` tests.

```bash
./run_tests.sh
```
