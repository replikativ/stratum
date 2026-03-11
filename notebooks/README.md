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
    ├── csv_import.clj
    ├── columnar_internals.clj
    └── api_reference.clj

clay.edn                      # Clay config (Quarto theme, target paths)
```

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

When Clay renders the notebook, `kind/test-last` runs the predicate
against the preceding expression's value. If it fails, the build fails
— so the rendered docs are always correct.

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

The notebooks are on the `:test` classpath. The test runner picks up
any `deftest` forms in them. The `kind/test-last` assertions run
during Clay rendering — they don't need the test runner.

```bash
./run_tests.sh
```
