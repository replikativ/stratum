# 65 — XTDB users and bitemporal use cases (benchmark feedstock)

**Date:** 2026-05-17
**Author:** research agent (for stratum benchmark planning)
**Status:** point-in-time survey, not a design doc

## TL;DR

- **Public XTDB customer logos are thin.** JUXT lists ~13 *industry vignettes* on
  `xtdb.com/solutions`, but only a handful name customers publicly: **Avisi
  Apps** (Atlas CRM in Atlassian Marketplace), **Lightpad** (notes/tasks +
  payments), **The Sample** (CMS startup on Biff), **Linux Foundation Egeria**
  (metadata governance via the `egeria-connector-xtdb`), plus an unnamed
  **state-owned energy retailer** (Virtual Power Plant) and an unnamed **large
  exchange** (spoofing/layering surveillance). Most case studies stay anonymised
  behind "tier-1 investment bank", "European Investment Bank", "Major US
  Investment Bank", etc.
- **XTDB v2 GA shipped 2025-06-12** under MPL, columnar Arrow storage, HTAP
  positioning, SQL-first with optional XTQL. v2.1 added secondary databases;
  recent beta work (`v2.0.0-beta7`) repartitioned L1 of the LSM tree by
  *recency* specifically to make sensor / pricing-feed time-travel cheap.
- **Stand-out reference customer: Nubank/Datomic** (~2.5 B tx/day, immutable
  audit trail). Not XTDB, but it sets the workload bar that any audit-grade
  bitemporal kernel is implicitly benchmarked against.
- **There IS a real academic bitemporal benchmark: TPC-BiH** (Kaufmann et al.,
  TPCTC 2013, SAP HANA team). It extends TPC-H with time-travel, temporal
  joins, temporal aggregation, and bulk-supersession updates. **This is the
  single most-citable benchmark to lead with**; nothing in the TPC mainline
  has replaced it since.
- **Regulatory drivers that explicitly imply bitemporality**: MiFID II
  trade-reconstruction within 72 h, Solvency II historical reserve
  reconstruction, SOX/IFRS prior-period restatement, FDA 21 CFR Part 11 audit
  trails, HIPAA modification logging, GDPR right-to-rectification. None of
  these mandate *the word* "bitemporal", but all require "what did the system
  know at time T, and what was true at time T'?" — which is the bitemporal
  predicate.
- **Audience for stratum benchmarks**: (1) regulated FS shops considering an
  XTDB-shaped audit substrate, (2) accounting/ERP vendors needing
  prior-period correction without rewriting history, (3) insurance back-office
  (claims with effective dates), (4) IoT/energy late-telemetry, (5)
  EHR/clinical with backdated events.

---

## 1. Prominent XTDB users (documented vs inferred)

| Customer | Domain | What we know | Source |
|---|---|---|---|
| **Avisi Apps — Atlas CRM** | Atlassian Marketplace CRM | XTDB used as engine behind Atlas CRM (Jira-integrated CRM). Cited as benefitting from pluggable storage (RocksDB / KV / S3) across cloud, on-prem, and hybrid deployments. No volume numbers published. | `xtdb.com/solutions`; Avisi Apps marketplace pages |
| **Lightpad** | Notes + payments SaaS | Standalone XTDB-on-RocksDB for payment data. Owner-reported "not a single faulty transaction in over a year". Plans to expose valid-time for note version history. Pre-revenue scale. | `lightpad.ai/w/blog/500592900` |
| **Linux Foundation Egeria** | Open metadata governance (LF AI & Data) | Production-ready `egeria-connector-xtdb` is the *native historical-metadata repo* for Egeria. Bitemporal needed because metadata edits arrive out of order across federated systems. | `github.com/odpi/egeria-connector-xtdb`; `egeria-project.org/connectors/repository/xtdb/` |
| **The Sample** | Content CMS startup on Biff | XTDB embedded via Biff framework. Small-scale, pilot/SaaS. | `xtdb.com/solutions` |
| **Unnamed state-owned energy retailer** (described, not named) | Virtual Power Plant orchestrating customer-owned solar/batteries | Bitemporality required for late telemetry, retroactive corrections, regulatory mandate to reconstruct *what the dispatch system knew at the moment of every dispatch decision*. Thousands of distributed assets. | `xtdb.com/solutions` |
| **Unnamed large exchange** (described, not named) | Market-surveillance for spoofing/layering | Bitemporal order-book replay to millisecond resolution. AI agents emit queries like "5+ rapid cancellations by trader X followed by favourable price move". | griddynamics.com bitemporal RBoR brief; FINOS OSFF blog |
| **Christopher Browne — Meridian** | Equity-derivatives post-trade risk (tier-1 IB veteran) | Solo-built valuation-and-risk system on XTDB. Each "valuation tag" is an immutable manifest of every input (spot, vol surface, rates, trade set, valuation date) so any run is reproducible from the tag alone. FINOS CDM data model. No public volume figures. | `juxt.pro/blog/building-meridian/` |
| **FINOS / Morgan Stanley TraderX** | Educational reference trading system | JUXT augmented TraderX with XTDB for FINOS 2024 Tech Sprint. Demonstrates blotter history, retroactive trade amendment, as-of pricing slider. Educational, not production. | `juxt.pro/blog/bitemporal-traderx/`, part 2 |
| **JUXT itself / Grid Dynamics** | Vendor + integrator (JUXT acquired by Grid Dynamics 2024-09) | JUXT FINOS Silver Member alongside G-Research, State Street, Temporal. Grid Dynamics distributes XTDB on AWS Marketplace. | `aws.amazon.com/marketplace/pp/prodview-iokxqykgxqe7i`; finos.org member list |

Industry vignettes on `xtdb.com/solutions` that **do not name a customer**:
e-commerce pricing for a "€300m+ revenue automotive distributor", a
multi-tenant insurance B2B2C platform, a Corda-based asset marketplace, a
multi-tenant manufacturing IoT/authz platform, banking fraud-detection,
HFT/forex/predictive-market trading systems. Treat these as *aspirational*
unless cross-referenced.

**Inferred-not-documented**: Grid Dynamics' Fortune-500 client base implies
several large undisclosed XTDB engagements post-acquisition, but I found no
named logos in 2024-2026 sources.

---

## 2. High-value bitemporal use-case catalog (by industry)

### 2.1 Financial services — trading & risk

- **Trade reconstruction (MiFID II)**: Reg requirement to reconstruct full
  trade lifecycle within 72 h of regulator request; records must be WORM-stored
  ≥ 5 years. *Workload shape*: append-heavy ingest of trades + orders + market
  data, point-in-time replay across millions of events, cross-table joins
  with independent as-ofs.
- **Position-history correction (P&L restatement)**: Big-R/Little-r prior-period
  adjustments are SEC- and GAAP-mandated. *Workload shape*: 10⁵–10⁷ positions,
  small fraction (1–5%) corrected within a fiscal year, audit query must show
  both the wrong and corrected numbers as known on each reporting date.
- **Post-trade risk valuation (Meridian-shape)**: Reproduce a valuation run from
  an "immutable tag" months later. *Workload shape*: thousands of trades per
  run, dozens of curves, valuation dates dense over recent months, sparse
  going back years.
- **Market surveillance (spoofing/layering)**: Replay order book to ms.
  *Workload shape*: 10⁸–10¹⁰ messages/day, 1-3 month hot window, range-scan
  by (instrument, time-range) is the dominant access pattern.
- **Bitemporal Risk Book of Record (RBoR)**: Grid Dynamics' marketed scenario
  for institutional portfolio mgmt. Sub-second marginal-VaR with as-of, Monte
  Carlo liquidity shocks. Cross-public-and-private assets.

### 2.2 Financial services — banking / fintech

- **Audit-grade account ledgers (Nubank-on-Datomic shape)**: 2.5 B tx/day,
  immutable history with as-of-then queries surfacing complete txn provenance
  (e.g. tip added later, FX rate change). Datomic, not XTDB, but it defines
  the workload bar. Source: `datomic.com/nubanks-story.html`.
- **Subscription billing reconciliation (Lightpad-shape)**: Valid-time-driven
  subscription schedules where webhook events from multiple payment gateways
  arrive out of order.

### 2.3 Insurance

- **Backdated policies & endorsements**: Most claims-made policies have a
  *retroactive date*; endorsements can shift it; life insurance permits
  backdating up to 6 months. *Workload shape*: low volume per policy
  (10s of versions), but every claim adjudication is a point-in-time query
  ("what coverage was in force on the date-of-loss, as known on the
  adjudication date").
- **Solvency II historical reserves**: EU insurers must hold and reproduce
  historical data sufficient to recompute technical provisions. Annual
  reporting + ad-hoc supervisory reconstruction.

### 2.4 Energy / IoT

- **Late-telemetry VPP (XTDB customer-shape)**: Customer-owned solar/battery
  devices report telemetry hours-to-days late; dispatch decisions must be
  replayable as the system knew them. *Workload shape*: 10⁴-10⁵ devices,
  high append rate (10s of Hz/device), heavy supersession of older readings,
  regulator-mandated as-of-dispatch replay.

### 2.5 Healthcare

- **Backdated clinical events**: A note dated yesterday transcribed today;
  malpractice defence requires both. HIPAA expects modification logs; FHIR
  uses `AuditEvent` and `Provenance` resources but offers no native
  bitemporal axis (downstream stores must provide it).
- **Pharma adverse-event reporting (FDA 21 CFR Part 11)**: WORM audit trail
  of clinical decisions; queries reconstruct "what was known about
  drug-interaction X on date Y".

### 2.6 Compliance / data governance

- **GDPR right-to-rectification**: Subject corrections must propagate while
  preserving the prior record for audit. Bitemporal "rectified on T, was
  thought-correct from T'" is the cleanest model.
- **Egeria metadata governance**: Federated metadata corrections from
  upstream tools arrive out of order; native historical metadata repo
  shipped on XTDB.

### 2.7 Accounting / ERP (kontor relevance)

- **Prior-period adjustments**: GAAP/IFRS distinguish Big-R (restate prior
  statements, withdraw old) from Little-r (revise). Both demand the ability
  to query "what did the trial balance show as known on close-date T₁?" and
  "as known on adjustment-date T₂".
- **VAT/sales-tax recomputation**: A retroactive rate-table correction must
  not silently rewrite historical postings; valid-time on the tax-rate
  table is the canonical representation.

---

## 3. Pain points users / observers report

Caveat: XTDB's public pain-point footprint is small (low review-site
presence, small forum, friendly HN threads). Most of the below is from
HN threads and the XTDB dev diary itself.

- **v1 upgrade pain (historical)**: A production user reported RocksDB
  reindexing for a 600 GB v1 dataset took "days to weeks" on a minor-version
  bump (HN, 2023). v2 is supposed to eliminate this via incremental indexing
  on raw data + ephemeral tx-log.
- **L0 read amplification (v2)**: XTDB's own dev posts acknowledge that
  point-lookups must scan every L0 file until background compaction
  consolidates. They added recency-partitioned L1 in `2.0.0-beta7` to
  prune frequently-updated entities (sensor/pricing).
- **No published TPC-H or bitemporal benchmark numbers**. The XTDB team
  runs nightly graph-query / bitemporal / ingestion / disk benchmarks on
  m5.xlarge but does not publish absolute numbers — only "10-12× ingest
  and ~8× OLAP vs v1" relative claims (XTDB dev diary #11).
- **Sparse third-party verification**. HN commenters have explicitly called
  out the lack of Jepsen-style testing on XTDB (Datomic has one; XTDB does
  not, as of the threads I found).
- **TraderX dev experience friction**: JUXT's own post notes that joining
  multiple tables each with their own `FOR VALID TIME AS OF …` requires
  "a small amount of ceremony" — i.e. the SQL surface for multi-table
  as-ofs is awkward.
- **Niche perception**: 2024 HN thread on "7 DBs in 7 Weeks" — author
  considered XTDB but skipped as "quite niche", would have displaced
  TigerBeetle.
- **Adjacent-market signal (algo trading DB shootout, Proof Trading,
  2020-ish)**: TimescaleDB OOM'd at 35 M trade load; ClickHouse fast but
  rejected for "no window functions / no updates"; DolphinDB fastest as-of
  joins (358 ms for SPY, 25 s all symbols on 35 M trades + 719 M quotes)
  but proprietary. MemSQL/SingleStore selected. **Implication for stratum**:
  the *as-of join* is the single most differentiating bitemporal operator
  for FS buyers; benchmark it explicitly.

---

## 4. Existing benchmarks (this is the gold)

### TPC-BiH (Kaufmann, Fischer et al., TPCTC 2013) — *the* bitemporal benchmark

- Extends **TPC-H** schema and queries with two added time dimensions
  (valid + system).
- Workload mix designed *jointly* by SAP HANA team + academic temporal-DB
  researchers, drawing from "real-life temporal applications from SAP's
  customer base" plus systematic coverage of temporal operators from the
  literature.
- Defines several **query classes** the spec calls out: time-travel
  point-lookup, time-travel range, temporal join, temporal aggregation,
  current-snapshot, history-of-entity.
- Defines **update workloads**: bulk supersession (e.g. mass price change),
  back-dated corrections, current-time append, future-dated insertion.
- Companion follow-up "Benchmarking Bitemporal Database Systems: Ready for
  the Future or Stuck in the Past?" (Kaufmann et al.) ran TPC-BiH against
  commercial temporal extensions and found **"support for temporal data is
  still in its infancy"** — most systems stored data in statically
  partitioned regular tables with rewrite-based query support.
- **Why this matters for stratum**: this benchmark is published, citable,
  vendor-neutral, has reference numbers. Even partial TPC-BiH conformance
  is a stronger marketing position than ad-hoc benchmarks.

### Adjacent benchmarks worth knowing

- **TPC-H** — base for TPC-BiH; OLAP star-schema with 22 queries. Stratum
  is columnar, so vanilla TPC-H is table-stakes.
- **TSBS** (Time-Series Benchmark Suite, Timescale-originated) — what
  kdb/KX, QuestDB, ClickHouse, Timescale, Influx all publish numbers
  against. Not bitemporal but is the *credibility benchmark for the
  time-series buyer*.
- **TPC-DI** — data integration, has SCD-2 elements but isn't a
  bitemporal benchmark in the TPC-BiH sense.

**Recommendation**: lead with TPC-BiH (port it / cite it), and supplement
with TSBS for the time-series buyer who recognises that suite.

---

## 5. Proposed benchmark scenarios for stratum

Each scenario is named, sourced from a real use-case above, sized
realistically, and tied to what it lets a buyer say about stratum.

| ID | Name | Source use-case | Data shape | Workload | What it measures | Buyer it convinces |
|---|---|---|---|---|---|---|
| **B1** | TPC-BiH-Mini | Academic TPC-BiH (TPCTC '13) | TPC-H @ SF=1, +VT/+ST columns | Mixed TT lookup / TT range / temporal join / temporal aggregation / bulk supersession | Standardised bitemporal SQL throughput — citable | Anyone evaluating bitemporal SQL engines |
| **B2** | Position-history correction | FS prior-period adjustment, Meridian | 10⁵ instruments × 10³ avg versions = 10⁸ rows; 5% rows superseded once, 0.5% twice; 1 y span | Hot-key as-of point-lookup at 1d / 30d / 6m / 1y back; concurrent correction inserts | Bitemporal point-lookup with deep history + concurrent writes | Quant-fund middle office, post-trade risk |
| **B3** | Sensor + late telemetry | XTDB VPP unnamed-energy-retailer | 10⁴ devices × 10 Hz × 30 d ≈ 2.6 × 10⁹ rows; 5% late by ≥ 1 h | Range-scan (device, time-range) as-of-then; supersession by replacement; full-fidelity replay of a 1-h dispatch window | Time-series append + supersession + as-of replay (recency-partitioned LSM stress) | Utilities, IoT platforms, grid operators |
| **B4** | Trade-reconstruction window | MiFID II / OSFF spoofing case | 10⁹ msgs/d; 1 instrument, 1 h replay window | Order-book reconstruction by replay of msg log filtered to (instrument, ms-window), with as-of-then; concurrent ingest | OLAP-style range-scan over time-partitioned data with bitemporal filter | Exchanges, market-surveillance teams |
| **B5** | Ledger with prior-period adj | Kontor / Nubank shape | 10⁷ postings / yr, ~100 accounts, 3 y horizon; 0.1% retro-adjusted, 0.01% Big-R restated | Trial balance as-of-date by account-tree; aggregate over period × valid range; mass back-dated insert (period close) | Bitemporal aggregation with hierarchical grouping (audit story for the buyer) | Accounting SaaS, ERP, finance teams |
| **B6** | Insurance policy-effective | Insurance retroactive-date workload | 10⁶ policies × ~5 endorsements each; 3% have effective-date shifts post-issue | "What coverage was in force on date-of-loss T₁, as known on adjudication date T₂?" Hot-pair point-lookup | Two-axis point-lookup; cross-join of policy + endorsement timelines | Insurance back-office / claims platforms |
| **B7** | Cross-table as-of join | TraderX friction; "small ceremony" pain | 3 tables (trades, prices, accounts), each with independent VT/ST | Multi-table join with *different* as-of per table | Tests whether the SQL surface and planner make this cheap | FS dev teams; differentiates vs vanilla SQL:2011 |
| **B8** | Audit-grade ingest sustained | Datomic-Nubank bar (2.5 B tx/d ≈ 30k tx/s) | Wide schema (~40 attrs), ~1 KB rows | 30k sustained append/s for 1 h, with on-the-fly compaction | Tests write path + compaction stability | Fintech, audit-substrate buyers |
| **B9** | Hot-narrow / cold-wide | XTDB recency-partition motivation | 10² hot entities receiving 10⁶ updates each + 10⁸ cold rarely-updated entities | Point-lookup on hot key at recent VT; range-scan on cold keys at historical VT | Tests recency partitioning, LSM compaction efficacy | Sensor/pricing-feed workloads |
| **B10** | GDPR rectification + tombstone | Compliance use-case | 10⁶ entities; 0.01% rectified, 0.001% purged | Mixed rectification (new fact replaces old) + purge (audit-preserved deletion); subsequent as-of-then query must show the rectification but the purged data must be unreadable | Tests the *only* operation a bitemporal store can't trivially do (true delete with bitemporal preservation of the delete event) | Compliance / regulated SaaS buyers |

### Suggested headline benchmarks

- **B1 (TPC-BiH)** — for the technical evaluator who wants apples-to-apples.
- **B2 (position-history correction)** — speaks directly to JUXT/Meridian
  audience.
- **B5 (ledger with prior-period adj)** — speaks directly to kontor's own
  audience and to Nubank-style fintech.
- **B7 (cross-table as-of join)** — directly addresses the documented
  TraderX friction; differentiates if stratum's surface is cleaner.

---

## 6. Audience implications

Given the use-case catalog, the realistic stratum buyer segments rank:

1. **Audit-grade fintech & accounting platforms** (kontor's own consumers,
   Nubank-style ledgers, billing/subscription tools). The *immutable +
   bitemporal* story sells itself; the buyer reflex is "we'd rather not
   build this ourselves". Lead with **B5, B8, B2**.
2. **Post-trade risk & valuation at FS firms** (Meridian-shape, RBoR-shape,
   tier-1-IB internal tooling). The buyer is a senior dev/architect who
   was at one of the prior shops. Lead with **B2, B7, B1**.
3. **Insurance back-offices** (claims, policy admin). Slower buyer cycle,
   but the *retroactive-date* model maps cleanly to bitemporal. Lead with
   **B6, B5**.
4. **Energy/IoT with regulatory replay** (VPP, smart-grid). Lead with
   **B3, B9**.
5. **Regulated SaaS (healthcare, GDPR)**: lead with **B10, B1**.

The two audiences XTDB v2 explicitly markets to that stratum could *also*
target are (a) FS compliance teams (MiFID II / SEC reconstruction) and
(b) data-platform teams that need HTAP without an OLAP sidecar. For both,
TPC-BiH conformance (or partial-conformance with a clean report) is
the single highest-leverage marketing investment per dollar.

---

## 7. Documented vs inferred — index

**Documented (named customer, public source):** Avisi Apps, Lightpad, Egeria,
The Sample, FINOS TraderX, Meridian, JUXT-as-Grid-Dynamics. Nubank for the
adjacent Datomic comparison.

**Documented but anonymous:** state-owned energy retailer (VPP), large
exchange (surveillance), €300m+ automotive distributor (e-commerce pricing),
European Investment Bank (JUXT case study, but not XTDB-tagged in the
visible case-study page — Datomic appears in 3 case studies on
`juxt.pro/case-studies`).

**Inferred only**: Grid-Dynamics Fortune-500 engagements, JUXT's
G-Research/State-Street FINOS-co-member proximity.

---

## 8. Key citations

- XTDB solutions catalog — https://www.xtdb.com/solutions
- XTDB v2 GA launch — https://xtdb.com/blog/launching-xtdb-v2 (2025-06-12)
- XTDB v2 release — https://github.com/xtdb/xtdb/releases/tag/v2.0.0
- XTDB dev diary #11 (perf claims) — https://www.xtdb.com/blog/dev-diary-mar-24
- XTDB performance index — https://v1-docs.xtdb.com/resources/performance/
- v2.0.0-beta7 (recency-partitioned LSM) — https://github.com/orgs/xtdb/discussions/4383
- Bitemporal Index storage post — https://xtdb.com/blog/building-a-bitemp-index-3-storage
- Bitemporal TraderX part 1 — https://www.juxt.pro/blog/bitemporal-traderx/
- Bitemporal TraderX part 2 — https://www.juxt.pro/blog/bitemporal-traderx-part2/
- Meridian case study — https://www.juxt.pro/blog/building-meridian/
- Bitemporal Risk Book of Record (Grid Dynamics) — https://www.griddynamics.com/blog/bitemporal-risk-book-of-record
- FINOS OSFF blog (XTDB & reporting) — https://www.finos.org/blog/osff-insights-how-can-modern-database-software-simplify-reporting
- JUXT compliance webinar — https://www.juxt.pro/blog/regulatory-compliance-xtdb-webinar/
- Lightpad on XTDB — https://lightpad.ai/w/blog/500592900
- Egeria XTDB connector — https://github.com/odpi/egeria-connector-xtdb
- HN XTDB 2.x early-access (upgrade pain anecdote) — https://news.ycombinator.com/item?id=35733515
- HN XTDB 2.0 GA — https://news.ycombinator.com/item?id=44268157
- HN "no bitemporal DB included" (niche perception) — https://news.ycombinator.com/item?id=42337408
- TPC-BiH paper — https://websci.informatik.uni-freiburg.de/publications/TPCTC-2013-TPC-BiH.pdf
- TPC-BiH chapter (Springer) — https://link.springer.com/chapter/10.1007/978-3-319-04936-6_2
- Benchmarking Bitemporal Database Systems (Kaufmann et al.) — https://www.researchgate.net/publication/262535409_Benchmarking_Bitemporal_Database_Systems_Ready_for_the_Future_or_Stuck_in_the_Past
- Bi-temporal Timeline Index (IEEE) — https://ieeexplore.ieee.org/document/7113307
- Datomic / Nubank story — https://www.datomic.com/nubanks-story.html
- Cognitect Nubank case study — https://www.cognitect.com/nubank-case-study.html
- Proof Trading DB shootout — https://medium.com/prooftrading/selecting-a-database-for-an-algorithmic-trading-system-2d25f9648d02
- MarkLogic bitemporal overview — https://www.progress.com/blogs/bitemporal
- Snowflake Time Travel docs — https://docs.snowflake.com/en/user-guide/data-time-travel
- SQL Server temporal tables — https://learn.microsoft.com/en-us/sql/relational-databases/tables/temporal-tables
- SQL Server temporal usage scenarios — https://learn.microsoft.com/en-us/sql/relational-databases/tables/temporal-table-usage-scenarios
- Insurance retroactive date (IRMI) — https://www.irmi.com/term/insurance-definitions/retroactive-date
- MiFID II record-keeping (Steel Eye) — https://www.steel-eye.com/regulators/mifid
- Solvency II data challenges (DataGalaxy 2026) — https://www.datagalaxy.com/en/blog/solvency-ii-compliance-in-2026/
- HIPAA temporal-tables case study (Medium) — https://medium.com/@keshavagrawal/building-a-hipaa-grade-audit-logging-system-lessons-from-the-healthcare-trenches-d5a8bb691e3b
- Aiven bitemporal overview — https://aiven.io/blog/two-dimensional-time-with-bitemporal-data
- Bitemporal modeling (Wikipedia) — https://en.wikipedia.org/wiki/Bitemporal_modeling
