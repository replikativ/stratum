(ns stratum.server.state
  "Durable server-level state, persisted via Konserve.

   Until this namespace existed, the PgWire server's `:registry` atom was
   the sole source of truth for: SQL-created tables, trained models, the
   live-table name → store mapping, and any future server-scoped
   declarations (ENUM, DOMAIN, type OIDs). All of it lived in heap. On
   restart the atom was reopened empty and clients saw `Unknown table
   'foo'` with no explanation — the data behind file-indexed stores and
   datasets was still durable, but the *binding* from a SQL name to it
   was gone.

   Pattern mirrors pg-datahike's `datahike.pg.server`: one durable
   record per named thing, idempotent install at start, in-memory cache
   loaded once and mutated in lockstep with each write. Read-on-demand
   only at startup; subsequent reads hit the atom.

   Konserve key layout

     [:server :meta]                       singleton  {:schema-version Long}
     [:server :next-oid]                   counter    Long  (next free user-type OID)
     [:server :sql-tables <table-name>]    per-name   {:dataset-uuid UUID
                                                       :branch       String?
                                                       :n-cols       Long
                                                       :temporal-units {col-kw → unit-kw}}
     [:server :enums <enum-name>]          per-name   {:values-ordered [String...]
                                                       :oid            Long}
     [:server :models <model-name>]        per-name   {:type kw :blob ...}
     [:server :live-tables <table-name>]   per-name   {:source {:type :file :path String}
                                                       :branch String}

   Future keys land under [:server :<section> ...] following the same
   shape. The schema-version singleton enables future forward-migration."
  (:require [konserve.core :as k]))

;; ---------------------------------------------------------------------------
;; Constants

(def SCHEMA-VERSION
  "Bumped when the durable layout changes incompatibly. Read at start;
   future code may compare and migrate."
  1)

(def FIRST-USER-OID
  "First OID handed out for user-declared types (ENUM, DOMAIN, …).
   Mirrors pg-datahike's allocation scheme and stays clear of system
   OIDs (PG reserves <= 16383)."
  0x40000000)

(def SECTIONS
  "Sections under [:server <section> <name>]. Used for hydration scans."
  [:sql-tables :enums :models :live-tables])

;; ---------------------------------------------------------------------------
;; Schema install / inspect

(defn schema-meta
  "Return the singleton meta map, or nil if uninstalled."
  [store]
  (k/get store [:server :meta] nil {:sync? true}))

(defn ensure-schema!
  "Idempotently install the top-level meta singleton + OID counter. Safe
   to call on every `start`. Returns the meta map."
  [store]
  (let [existing (schema-meta store)]
    (cond
      (nil? existing)
      (let [meta {:schema-version SCHEMA-VERSION}]
        (k/assoc store [:server :meta] meta {:sync? true})
        (k/assoc store [:server :next-oid] FIRST-USER-OID {:sync? true})
        meta)

      (= SCHEMA-VERSION (:schema-version existing))
      existing

      :else
      (throw (ex-info (str "Stratum server-state schema version mismatch: "
                           "store has " (:schema-version existing)
                           ", code expects " SCHEMA-VERSION)
                      {:store-version (:schema-version existing)
                       :code-version  SCHEMA-VERSION})))))

;; ---------------------------------------------------------------------------
;; OID allocator (atomic via k/update)

(defn allocate-oid!
  "Allocate the next free user-type OID and persist the bumped counter.
   Atomic at the Konserve layer via `k/update`."
  ^long [store]
  (let [[_ new-val] (k/update store [:server :next-oid]
                              #(inc (or % FIRST-USER-OID))
                              {:sync? true})]
    (long new-val)))

;; ---------------------------------------------------------------------------
;; Per-section CRUD

(defn put!
  "Write one record under [:server <section> <name>]. Overwrites if
   present — caller is responsible for IF-NOT-EXISTS / IF-EXISTS
   policy upstream."
  [store section name record]
  (k/assoc store [:server section name] record {:sync? true})
  record)

(defn get-record
  "Read one record by section + name. Returns nil if absent."
  [store section name]
  (k/get store [:server section name] nil {:sync? true}))

(defn delete!
  "Remove one record under [:server <section> <name>]. No-op if absent."
  [store section name]
  (k/dissoc store [:server section name] {:sync? true}))

(defn list-section
  "Return `{name → record}` for all entries under [:server <section> …]."
  [store section]
  (let [key-infos (k/keys store {:sync? true})
        names     (->> key-infos
                       (map :key)
                       (filter #(and (vector? %)
                                     (= 3 (count %))
                                     (= :server (first %))
                                     (= section (second %))))
                       (map #(nth % 2)))]
    (into {}
          (keep (fn [name]
                  (when-let [rec (get-record store section name)]
                    [name rec])))
          names)))

;; ---------------------------------------------------------------------------
;; Snapshot / hydrate

(defn snapshot
  "Read all sections into a single map for cache hydration at start:

     {:meta    {:schema-version Long}
      :next-oid Long
      :sql-tables {name → record}
      :enums      {name → record}
      :models     {name → record}
      :live-tables{name → record}}"
  [store]
  (-> (into {} (map (fn [section] [section (list-section store section)])) SECTIONS)
      (assoc :meta     (schema-meta store))
      (assoc :next-oid (k/get store [:server :next-oid] FIRST-USER-OID {:sync? true}))))
