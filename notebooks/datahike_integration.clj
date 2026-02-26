;; # Datahike + Stratum: Entity DB meets Columnar Analytics

;; authors: Christian Weilbach

;; last change: 2026-02-17

;; ## The Problem

;; You have an application backed by Datahike — products, customers, orders,
;; all modeled as entities with schema, refs, and Datalog queries. Now the
;; business asks: "What's our revenue by region last month? Which products
;; are trending? Show me a dashboard."
;;
;; Datahike can answer these, but scanning millions of entities row-by-row
;; through Datalog is slow for aggregation. You need a columnar engine.
;;
;; **Stratum** sits alongside Datahike as an analytical cache:
;;
;; | Concern | Datahike | Stratum |
;; |---------|----------|---------|
;; | Data model | Entities with refs | Typed columns (long[], double[], String[]) |
;; | Queries | Datalog (graph traversal, joins) | SIMD filter+aggregate, GROUP BY |
;; | Strength | "Find order #42 and its customer" | "SUM(revenue) GROUP BY region" |
;; | Mutation | ACID transactions | Copy-on-write snapshots |
;;
;; The pattern: Datahike is the source of truth, Stratum is the analytics
;; materialization. After each batch of Datahike transactions, you sync
;; the relevant columns to Stratum.

;; ## Setup

(ns datahike-integration
  (:require [stratum.api :as st]
            [stratum.query :as q]
            [stratum.dataset :as dataset]
            [stratum.yggdrasil :as ygg]
            [stratum.storage :as storage]
            [stratum.server :as server]
            [datahike.api :as d]
            [yggdrasil.protocols :as p]
            [konserve.store :as kstore]
            [scicloj.kindly.v4.kind :as kind]))

;; ## 1. Datahike: The Entity Layer

;; A small e-commerce database with products, customers, and orders.
;; Note the `:db.type/ref` links — this is where Datahike shines.

(def dh-config {:store {:backend :mem :id "shop-db"}
                :schema-flexibility :write
                :keep-history? true})

(when (d/database-exists? dh-config) (d/delete-database dh-config))
(d/create-database dh-config)
(def conn (d/connect dh-config))

(d/transact conn
  [;; Product schema
   {:db/ident :product/sku    :db/valueType :db.type/string  :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :product/name   :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
   {:db/ident :product/category :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :product/price  :db/valueType :db.type/double  :db/cardinality :db.cardinality/one}
   ;; Customer schema
   {:db/ident :customer/email :db/valueType :db.type/string  :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :customer/name  :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
   {:db/ident :customer/region :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   ;; Order schema — refs to product and customer
   {:db/ident :order/id       :db/valueType :db.type/long    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :order/product  :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}
   {:db/ident :order/customer :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}
   {:db/ident :order/quantity :db/valueType :db.type/long    :db/cardinality :db.cardinality/one}
   {:db/ident :order/timestamp :db/valueType :db.type/long   :db/cardinality :db.cardinality/one}])

;; Seed products and customers:

(d/transact conn
  [{:product/sku "WDG-001" :product/name "Widget Pro" :product/category "Hardware" :product/price 29.99}
   {:product/sku "GDG-002" :product/name "Gadget Plus" :product/category "Electronics" :product/price 49.99}
   {:product/sku "GZM-003" :product/name "Gizmo Lite" :product/category "Electronics" :product/price 19.99}
   {:product/sku "WDG-004" :product/name "Widget Mini" :product/category "Hardware" :product/price 14.99}
   {:customer/email "alice@example.com" :customer/name "Alice" :customer/region "US"}
   {:customer/email "bob@example.com" :customer/name "Bob" :customer/region "EU"}
   {:customer/email "carol@example.com" :customer/name "Carol" :customer/region "APAC"}])

;; First batch of orders (January):

(d/transact conn
  [{:order/id 1 :order/product [:product/sku "WDG-001"] :order/customer [:customer/email "alice@example.com"] :order/quantity 3 :order/timestamp 1704067200}
   {:order/id 2 :order/product [:product/sku "GDG-002"] :order/customer [:customer/email "bob@example.com"]   :order/quantity 1 :order/timestamp 1704153600}
   {:order/id 3 :order/product [:product/sku "GZM-003"] :order/customer [:customer/email "carol@example.com"] :order/quantity 5 :order/timestamp 1704240000}
   {:order/id 4 :order/product [:product/sku "WDG-001"] :order/customer [:customer/email "bob@example.com"]   :order/quantity 2 :order/timestamp 1704326400}
   {:order/id 5 :order/product [:product/sku "WDG-004"] :order/customer [:customer/email "alice@example.com"] :order/quantity 10 :order/timestamp 1704412800}])

;; Datahike excels at entity lookups — "find order #3 with its product and customer":

(d/pull @conn '[* {:order/product [:product/name :product/price]}
                   {:order/customer [:customer/name :customer/region]}]
        [:order/id 3])

;; ## 2. The Bridge: Datalog → Columnar

;; To feed Stratum, we denormalize with a Datalog query that flattens the
;; entity graph into positional tuples. Then `tuples->columns` converts
;; to typed arrays — the natural bridge between the two systems.

(defn datahike->stratum
  "Materialize a denormalized analytics view from Datahike into a Stratum column map.
   Uses Datalog to join entities, tuples->columns for type-inferred conversion."
  [db]
  (let [tuples (d/q '[:find ?oid ?product-name ?category ?price ?qty ?region ?ts
                       :where
                       [?o :order/id ?oid]
                       [?o :order/product ?p]
                       [?o :order/customer ?c]
                       [?o :order/quantity ?qty]
                       [?o :order/timestamp ?ts]
                       [?p :product/name ?product-name]
                       [?p :product/category ?category]
                       [?p :product/price ?price]
                       [?c :customer/region ?region]]
                     db)]
    (st/tuples->columns tuples [:order_id :product :category :price :quantity :region :timestamp])))

;; Materialize current state:

(def analytics-v1 (datahike->stratum @conn))

;; This is a plain Clojure map of typed arrays — ready for Stratum queries:

(keys analytics-v1)
;; => (:order_id :product :category :price :quantity :region :timestamp)

;; ## 3. Stratum Analytics: DSL Queries

;; Now we can run fast analytical queries. These compile to SIMD-accelerated
;; Java hot paths — the same engine that beats DuckDB on TPC-H benchmarks.

;; **Revenue by region:**

(st/q {:from analytics-v1
      :group [:region]
      :agg [[:as [:sum [:* :price :quantity]] :revenue]
            [:as [:count] :orders]]})

;; **Top products by units sold:**

(st/q {:from analytics-v1
      :group [:product]
      :agg [[:as [:sum :quantity] :units_sold]
            [:as [:avg :price] :avg_price]]
      :order [[:units_sold :desc]]})

;; **Category breakdown with statistics:**

(st/q {:from analytics-v1
      :group [:category]
      :agg [[:as [:sum [:* :price :quantity]] :revenue]
            [:as [:avg :price] :avg_price]
            [:as [:count] :order_count]]})

;; ## 4. Stratum Analytics: SQL Queries

;; The same data is queryable via SQL — useful for dashboards, BI tools,
;; or anyone who doesn't want to learn the DSL.

;; **SQL with inline table reference:**

(st/q "SELECT region, SUM(price * quantity) AS revenue, COUNT(*) AS orders
      FROM orders GROUP BY region ORDER BY revenue DESC"
     {"orders" analytics-v1})

;; **SQL with expressions:**

(st/q "SELECT product, SUM(quantity) AS units, AVG(price) AS avg_price
      FROM orders GROUP BY product HAVING SUM(quantity) > 2
      ORDER BY units DESC"
     {"orders" analytics-v1})

;; ## 5. Auto-Sync via `d/listen!`

;; Instead of manually re-materializing after each transaction, we can
;; use Datahike's `d/listen!` to automatically sync to Stratum on every
;; transaction. This is the production pattern — zero-effort consistency.

;; Set up persistent Stratum storage:

(def store-config {:backend :file
                   :path "/tmp/stratum-datahike-demo"
                   :id #uuid "660e8400-e29b-41d4-a716-446655440001"})

(when (kstore/store-exists? store-config {:sync? true})
  (kstore/delete-store store-config {:sync? true}))
(def store (kstore/create-store store-config {:sync? true}))

;; The sync helper: materialize → make dataset → ensure indexed → sync.
;; `with-parent` carries the commit-info from the previous sync so
;; `sync!` records the parent pointer, enabling time-travel.

(defn sync-analytics!
  "Materialize Datahike → Stratum and persist with tx metadata.
   prev-ds (optional): previous synced dataset for parent chain.
   Returns the synced StratumDataset."
  ([db store branch] (sync-analytics! db store branch nil))
  ([db store branch prev-ds]
   (let [tx-id (:max-tx db)
         columns (datahike->stratum db)
         ds (-> (st/make-dataset columns {:name "orders"
                                         :metadata {"datahike/tx" tx-id}})
                (st/ensure-indexed)
                (cond-> prev-ds (dataset/with-parent prev-ds)))]
     (dataset/sync! ds store branch))))

;; Install the listener — every Datahike transaction auto-syncs to Stratum:

(def ds-atom (atom nil))

(defn install-stratum-sync!
  "Install a Datahike listener that auto-syncs to Stratum on each tx.
   Uses the previous @ds-atom as parent for commit chain continuity."
  [conn ds-atom store branch]
  (d/listen conn :stratum-sync
    (fn [{:keys [db-after]}]
      (let [ds (sync-analytics! db-after store branch @ds-atom)]
        (reset! ds-atom ds)))))

(install-stratum-sync! conn ds-atom store "main")

;; Manually sync the current state (listener only fires on *new* transactions):

(reset! ds-atom (sync-analytics! @conn store "main"))

;; Now any new transaction will auto-sync. Let's add February orders:

(d/transact conn
  [{:order/id 6 :order/product [:product/sku "GDG-002"] :order/customer [:customer/email "alice@example.com"] :order/quantity 2 :order/timestamp 1706745600}
   {:order/id 7 :order/product [:product/sku "GZM-003"] :order/customer [:customer/email "carol@example.com"] :order/quantity 8 :order/timestamp 1706832000}
   {:order/id 8 :order/product [:product/sku "WDG-001"] :order/customer [:customer/email "bob@example.com"]   :order/quantity 4 :order/timestamp 1706918400}])

;; The listener fired — @ds-atom now has 8 orders, auto-synced:

(st/q {:from @ds-atom
      :agg [[:as [:sum [:* :price :quantity]] :total_revenue]
            [:as [:count] :order_count]]})

;; ## 6. Yggdrasil Composite: Atomic Snapshots

;; Both systems have their own versioning, but how do you ensure "analytics
;; at commit X corresponds to entities at tx Y"? Yggdrasil composites
;; capture both snapshot IDs together atomically.

(def stratum-sys (ygg/create-dataset-system @ds-atom store
                                             {:system-name "orders"}))

;; The Stratum system tracks the current dataset via an atom.
;; `d/listen!` updates the atom, and `snapshot-id` dereferences it:

(p/system-id stratum-sys)
;; => "orders"

(p/snapshot-id stratum-sys)
;; shows the UUID of the current dataset commit

;; Query a historical snapshot via as-of:

(let [snap-id (p/snapshot-id stratum-sys)
      historical-ds (p/as-of stratum-sys snap-id)]
  (st/q {:from historical-ds
        :group [:region]
        :agg [[:as [:sum [:* :price :quantity]] :revenue]]}))

;; Walk the commit graph:

(p/history stratum-sys)

;; ## 7. Time-Travel

;; Load the first commit (5 orders, before February batch):

(let [history (p/history stratum-sys)
      first-snap-id (last history) ;; oldest commit
      jan-ds (p/as-of stratum-sys first-snap-id)]
  ;; January analytics
  {:jan (st/q {:from jan-ds
              :agg [[:as [:count] :order_count]
                    [:as [:sum [:* :price :quantity]] :revenue]]})
   ;; Current analytics
   :current (st/q {:from @ds-atom
                   :agg [[:as [:count] :order_count]
                         [:as [:sum [:* :price :quantity]] :revenue]]})})

;; ## 8. SQL Access

;; The same data is queryable via SQL strings. Pass the StratumDataset
;; directly as a table value:

(st/q "SELECT region, SUM(price * quantity) AS revenue, COUNT(*) AS orders
      FROM orders GROUP BY region ORDER BY revenue DESC"
     {"orders" @ds-atom})

;; Or with the 3-arity path for storage-backed resolution:

(st/q "SELECT SUM(price * quantity) AS revenue, COUNT(*) AS orders FROM orders"
     {"orders" @ds-atom}
     {:store store})

;; ## 9. PgWire Server: Live Tables

;; For dashboards, register a "live table" that resolves from storage
;; on each query — always showing the latest synced data.

(comment
  ;; Start server on port 5433
  (def srv (st/start-server {:port 5433}))

  ;; Register as live table — resolves from storage on each query
  (st/register-live-table! srv "orders" store "main")

  ;; Now psql always sees the latest data:
  ;;
  ;;   psql -h localhost -p 5433 -U stratum
  ;;
  ;;   stratum=> SELECT region, SUM(price * quantity) AS revenue
  ;;             FROM orders GROUP BY region;
  ;;
  ;; After d/listen! auto-syncs new transactions, subsequent queries
  ;; automatically see the updated data — no re-registration needed.

  ;; Or register a static snapshot (classic mode):
  ;; (st/register-table! srv "orders" (dataset/columns @ds-atom))

  (st/stop-server srv))

;; ## 10. Branching: What-If Scenarios

;; "What if we raised Widget prices by 50%?" Fork the analytics,
;; modify, compare — without touching the source of truth.

(let [current (st/load store "main")
      scenario (dataset/fork current)]
  ;; The fork is O(1) — structural sharing, no data copied
  (dataset/sync! scenario store "what-if-pricing")
  (let [main-rev (st/q {:from (st/load store "main")
                        :agg [[:as [:sum [:* :price :quantity]] :revenue]]})
        whatif-rev (st/q {:from (st/load store "what-if-pricing")
                         :agg [[:as [:sum [:* :price :quantity]] :revenue]]})]
    {:main (:revenue (first main-rev))
     :what-if (:revenue (first whatif-rev))}))

;; ## 11. Cleanup

(d/unlisten conn :stratum-sync)
(storage/gc! store)

;; ## Architecture Summary

;; ```
;; ┌──────────────┐  d/listen!   ┌──────────────┐
;; │  Datahike    │────────────→ │   Stratum    │
;; │  (entities)  │  auto-sync   │  (columns)   │
;; │              │  materialize │              │
;; │  :order/id   │  + sync!     │  long[]      │
;; │  :order/product             │  double[]    │
;; │  :customer/region           │  String[]    │
;; └──────┬───────┘              └──────┬───────┘
;;        │ ACID tx                     │ SIMD analytics
;;        │ entity lookup               │ GROUP BY, JOIN
;;        │ graph traversal             │ filter+aggregate
;;        │                             │
;;        │    ┌──────────────────┐     │
;;        └───→│ Yggdrasil        │←────┘
;;             │ Composite        │
;;             │                  │
;;             │ atomic snapshots │
;;             │ time-travel      │
;;             │ branching        │
;;             └──────────────────┘
;; ```
;;
;; **Data flow:**
;; 1. Datahike is the source of truth (transactions, schema, refs)
;; 2. `d/listen!` fires on every transaction → `datahike->stratum` materializes
;; 3. `ensure-indexed` + `sync!` persists with Datahike tx-id metadata
;; 4. Yggdrasil composite captures both snapshot IDs atomically
;; 5. Analytics via DSL, SQL, or PgWire server (with `register-live-table!`)
;; 6. Time-travel: `p/as-of` returns queryable StratumDataset at any snapshot
;;
;; **When to use each:**
;; - "Find customer #42's latest order" → Datahike (entity lookup)
;; - "Revenue by region last quarter" → Stratum (columnar aggregate)
;; - "Dashboard for the ops team" → Stratum PgWire (psql/Grafana)
;; - "What did the data look like at tx 42?" → both via Yggdrasil composite
;;
;; **Scaling note:** This notebook uses full re-materialization (Datalog query
;; → tuples → columns). For production scale with high-frequency transactions,
;; incremental per-datom updates with entity-ID → row-position mapping and
;; transient indices would enable efficient append workloads.
