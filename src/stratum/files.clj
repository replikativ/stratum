(ns stratum.files
  "File indexing: materialize CSV/Parquet files into persistent Stratum
   indices stored in per-file konserve filestores.

   Each file gets its own store directory:
     <data-dir>/<filename>.stratum/

   On first access the file is read, all columns converted to
   PersistentColumnIndex (with zone maps), and the dataset synced to the
   store. Subsequent accesses are O(1) — the PSS tree is lazy and only
   chunks touched by a query are loaded from disk.

   Mtime-based cache invalidation: if the source file is newer than the
   stored commit, the file is re-indexed automatically."
  (:require [stratum.dataset :as dataset]
            [stratum.csv :as csv]
            [stratum.parquet :as parquet]
            [clojure.string :as str]
            [konserve.store :as kstore])
  (:import [java.io File]))

(defn- file-mtime ^long [^String path]
  (.lastModified (File. path)))

(defn file-store-dir
  "Returns the path for the per-file konserve store.
   e.g., data-dir=/tmp/stratum-data, file=/path/to/orders.csv
         → /tmp/stratum-data/orders.csv.stratum"
  ^String [^String data-dir ^String file-path]
  (str data-dir File/separator (.getName (File. file-path)) ".stratum"))

(defn- open-file-store
  "Open (or create) a per-file konserve filestore at store-dir.
   Uses store-exists? + connect-store / create-store for proper lifecycle."
  [^String store-dir]
  (.mkdirs (File. store-dir))
  (let [cfg {:backend :file
             :path    store-dir
             :id      (java.util.UUID/nameUUIDFromBytes (.getBytes store-dir "UTF-8"))}]
    (if (kstore/store-exists? cfg {:sync? true})
      (kstore/connect-store cfg {:sync? true})
      (kstore/create-store  cfg {:sync? true}))))

(defn- index-file-into-store!
  "Read file at abs-path, index all columns, persist to store under branch.
   Returns the synced StratumDataset."
  [^String abs-path store ^String branch ^long mtime]
  (let [ext (let [n abs-path i (.lastIndexOf n ".")]
              (when (>= i 0) (str/lower-case (.substring n i))))
        ds  (cond
              (= ext ".csv")
              (csv/from-csv abs-path)

              (= ext ".parquet")
              (parquet/from-parquet abs-path)

              :else
              (throw (ex-info (str "Unsupported file type '" ext
                                   "' — supported: .csv .parquet")
                              {:path abs-path})))]
    (-> ds
        dataset/ensure-indexed
        (dataset/with-metadata {:source-path  abs-path
                                :source-mtime mtime})
        (dataset/sync! store branch))))

(defn load-or-index-file!
  "Load a CSV or Parquet file as a fully-indexed StratumDataset.

   On first access — or when the source file is newer than the cached
   commit — reads the file, converts all columns to PersistentColumnIndex,
   and persists to a konserve store.

   Two calling conventions:

     (load-or-index-file! file-path data-dir)
       data-dir is a String. Creates a per-file filestore at
       <data-dir>/<filename>.stratum/. Used by the SQL server.

     (load-or-index-file! file-path {:store open-store})
       Uses an already-open konserve store (any backend: file, S3, Redis…).
       :branch defaults to the file's stem (e.g., \"orders\" for orders.csv).
       Use this from Clojure when you manage the store lifecycle yourself.

   Subsequent accesses load the lazy PSS tree from disk instantly.

   Supported: .csv, .parquet
   Returns a StratumDataset with index-backed columns and zone maps."
  [^String file-path store-or-data-dir]
  (let [abs-path (-> file-path File. .getAbsolutePath)
        [store branch]
        (if (string? store-or-data-dir)
          ;; data-dir string → open/create per-file filestore
          [(open-file-store (file-store-dir store-or-data-dir abs-path)) "main"]
          ;; opts map with :store and optional :branch
          (let [{:keys [store branch]} store-or-data-dir
                branch (or branch
                           (let [n (.getName (File. abs-path))
                                 i (.lastIndexOf n ".")]
                             (if (> i 0) (.substring n 0 i) n)))]
            [store branch]))
        mtime    (file-mtime abs-path)
        existing (try (dataset/load store branch) (catch Exception _ nil))
        cached-mt (when existing (get (:metadata existing) :source-mtime))]
    (if (and existing (= cached-mt mtime))
      ;; Cache hit — return lazy index-backed dataset
      existing
      ;; Cache miss or stale — re-read and index
      (index-file-into-store! abs-path store branch mtime))))
