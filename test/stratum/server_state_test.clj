(ns stratum.server-state-test
  "Restart-safety phase R1: foundation tests for `stratum.server.state` —
   schema install, OID allocation, per-section CRUD, snapshot/hydrate.

   Round-trip tests exercise both `new-mem-store` and the real file
   store, with the file store explicitly closed/re-opened to validate
   the durability claim."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as kstore]
            [konserve.memory :refer [new-mem-store]]
            [stratum.server.state :as state])
  (:import [java.io File]
           [java.util UUID]))

(defn- mem-store [] (new-mem-store (atom {}) {:sync? true}))

(defn- temp-dir ^String []
  (let [d (File/createTempFile "stratum-state-" "")]
    (.delete d)
    (.mkdirs d)
    (.getAbsolutePath d)))

(defn- delete-dir [^String path]
  (let [f (File. path)]
    (when (.exists f)
      (doseq [^File child (reverse (file-seq f))]
        (.delete child)))))

(defn- file-store-at [^String path]
  (let [cfg {:backend :file
             :path    path
             :id      (UUID/nameUUIDFromBytes (.getBytes path "UTF-8"))}]
    (if (kstore/store-exists? cfg {:sync? true})
      (kstore/connect-store cfg {:sync? true})
      (kstore/create-store  cfg {:sync? true}))))

;; ============================================================================
;; Schema install

(deftest ensure-schema!-installs-once
  (let [s (mem-store)]
    (is (nil? (state/schema-meta s)) "pre-install schema is absent")
    (let [installed (state/ensure-schema! s)]
      (is (= state/SCHEMA-VERSION (:schema-version installed))))
    (is (= state/SCHEMA-VERSION (:schema-version (state/schema-meta s))))))

(deftest ensure-schema!-idempotent
  (let [s (mem-store)]
    (state/ensure-schema! s)
    ;; Second call must not throw, must not bump anything
    (let [oid-1 (state/allocate-oid! s)
          _     (state/ensure-schema! s)
          oid-2 (state/allocate-oid! s)]
      (is (= 1 (- oid-2 oid-1)) "OID counter is preserved across ensure-schema!"))))

(deftest schema-version-mismatch-throws
  (let [s (mem-store)]
    (kstore/assoc s [:server :meta] {:schema-version 99999} {:sync? true})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"schema version mismatch"
                          (state/ensure-schema! s)))))

;; ============================================================================
;; OID allocator

(deftest allocate-oid!-monotonic
  (let [s (mem-store)]
    (state/ensure-schema! s)
    (let [first-oid (state/allocate-oid! s)]
      (is (= (inc state/FIRST-USER-OID) first-oid)
          "First allocation hands out FIRST-USER-OID + 1")
      (dotimes [i 5]
        (is (= (+ first-oid (inc i)) (state/allocate-oid! s)))))))

(deftest allocate-oid!-no-schema-still-works
  ;; allocate-oid! must default to FIRST-USER-OID even without ensure-schema!
  (let [s (mem-store)
        first-oid (state/allocate-oid! s)]
    (is (= (inc state/FIRST-USER-OID) first-oid))))

;; ============================================================================
;; CRUD per section

(deftest put-get-list-section-roundtrip
  (let [s (mem-store)]
    (state/ensure-schema! s)
    (state/put! s :enums "mood" {:values-ordered ["sad" "ok" "happy"]})
    (state/put! s :enums "color" {:values-ordered ["red" "green"]})
    (state/put! s :sql-tables "orders" {:dataset-uuid (UUID/randomUUID)})

    (is (= {:values-ordered ["sad" "ok" "happy"]}
           (state/get-record s :enums "mood")))
    (is (nil? (state/get-record s :enums "nonexistent")))
    (is (= #{"mood" "color"}
           (set (keys (state/list-section s :enums)))))
    (is (= #{"orders"}
           (set (keys (state/list-section s :sql-tables)))))
    (is (= {} (state/list-section s :models)) "empty section returns {}")))

(deftest sections-are-isolated
  ;; A name registered in :enums must not leak into :sql-tables (or vice versa)
  (let [s (mem-store)]
    (state/ensure-schema! s)
    (state/put! s :enums "foo" {:values-ordered ["a"]})
    (state/put! s :sql-tables "foo" {:dataset-uuid (UUID/randomUUID)})
    (is (= {:values-ordered ["a"]} (state/get-record s :enums "foo")))
    (is (some? (state/get-record s :sql-tables "foo")))
    (is (not= (state/get-record s :enums "foo")
              (state/get-record s :sql-tables "foo")))))

(deftest delete!-removes-record
  (let [s (mem-store)]
    (state/ensure-schema! s)
    (state/put! s :enums "mood" {:values-ordered ["a" "b"]})
    (is (some? (state/get-record s :enums "mood")))
    (state/delete! s :enums "mood")
    (is (nil? (state/get-record s :enums "mood")))
    ;; Deleting a missing record must not throw
    (state/delete! s :enums "nonexistent")))

;; ============================================================================
;; Snapshot

(deftest snapshot-includes-all-sections
  (let [s (mem-store)]
    (state/ensure-schema! s)
    (state/put! s :enums "mood" {:values-ordered ["a"]})
    (state/put! s :sql-tables "t" {:dataset-uuid (UUID/randomUUID)})
    (state/put! s :models "m" {:type :iforest :blob {}})
    (state/put! s :live-tables "l" {:source {:type :file :path "/tmp/x.csv"}
                                    :branch "main"})
    (let [snap (state/snapshot s)]
      (is (= #{:meta :next-oid :sql-tables :enums :models :live-tables}
             (set (keys snap))))
      (is (= 1 (count (:enums snap))))
      (is (= 1 (count (:sql-tables snap))))
      (is (= 1 (count (:models snap))))
      (is (= 1 (count (:live-tables snap))))
      (is (= state/SCHEMA-VERSION (-> snap :meta :schema-version))))))

;; ============================================================================
;; File-store round-trip — the core durability claim

(deftest file-store-survives-close-and-reopen
  (let [path (temp-dir)]
    (try
      ;; First session — write
      (let [s1 (file-store-at path)]
        (state/ensure-schema! s1)
        (let [oid (state/allocate-oid! s1)]
          (state/put! s1 :enums "mood" {:values-ordered ["sad" "ok" "happy"]
                                        :oid oid}))
        (state/put! s1 :sql-tables "orders"
                    {:dataset-uuid (UUID/fromString "abc12345-0000-0000-0000-000000000001")
                     :n-cols 3}))
      ;; Second session — read
      (let [s2 (file-store-at path)
            snap (state/snapshot s2)]
        (is (= state/SCHEMA-VERSION (-> snap :meta :schema-version)))
        ;; OID counter persisted
        (is (= (+ 1 state/FIRST-USER-OID) (:next-oid snap))
            ":next-oid persisted across session boundary")
        ;; Enum persisted with declaration order intact
        (is (= ["sad" "ok" "happy"]
               (-> snap :enums (get "mood") :values-ordered)))
        ;; Table binding persisted
        (is (= (UUID/fromString "abc12345-0000-0000-0000-000000000001")
               (-> snap :sql-tables (get "orders") :dataset-uuid))))
      (finally (delete-dir path)))))
