(ns
 stratum-book.columnar-internals-generated-test
 (:require
  [stratum.chunk :as chunk]
  [stratum.index :as index]
  [scicloj.kindly.v4.kind :as kind]
  [clojure.test :refer [deftest is]]))


(def v3_l30 (def c-f64 (chunk/make-chunk :float64 10)))


(def v4_l31 (def c-i64 (chunk/make-chunk :int64 10)))


(def v5_l33 (chunk/chunk-length c-f64))


(deftest t6_l35 (is ((fn [n] (= 10 n)) v5_l33)))


(def v7_l38 (chunk/chunk-datatype c-f64))


(deftest t8_l40 (is ((fn [t] (= :float64 t)) v7_l38)))


(def v9_l43 (chunk/chunk-datatype c-i64))


(deftest t10_l45 (is ((fn [t] (= :int64 t)) v9_l43)))


(def
 v12_l54
 (def
  c-rw
  (let
   [c (chunk/col-transient (chunk/make-chunk :float64 10))]
   (chunk/write-double! c 0 42.0)
   (chunk/write-double! c 5 3.14)
   (chunk/col-persistent! c)
   c)))


(def
 v13_l61
 [(chunk/read-double c-rw 0)
  (chunk/read-double c-rw 5)
  (chunk/read-double c-rw 1)])


(deftest
 t14_l65
 (is
  ((fn [[v0 v5 v1]] (and (= 42.0 v0) (= 3.14 v5) (= 0.0 v1))) v13_l61)))


(def
 v16_l73
 (try
  (chunk/write-double! (chunk/make-chunk :float64 10) 0 42.0)
  :no-error
  (catch IllegalStateException _ :threw)))


(deftest t17_l78 (is ((fn [r] (= :threw r)) v16_l73)))


(def
 v19_l87
 (def
  c-original
  (let
   [c (chunk/col-transient (chunk/make-chunk :float64 10))]
   (chunk/write-double! c 0 42.0)
   (chunk/col-persistent! c)
   c)))


(def
 v20_l93
 (def
  c-forked
  (let
   [c (chunk/col-transient (chunk/col-fork c-original))]
   (chunk/write-double! c 0 99.0)
   (chunk/col-persistent! c)
   c)))


(def
 v21_l99
 {:original (chunk/read-double c-original 0),
  :forked (chunk/read-double c-forked 0)})


(deftest
 t22_l102
 (is
  ((fn
    [{:keys [original forked]}]
    (and (= 42.0 original) (= 99.0 forked)))
   v21_l99)))


(def
 v24_l110
 (def
  c-base
  (let
   [c (chunk/col-transient (chunk/make-chunk :float64 10))]
   (chunk/write-double! c 0 42.0)
   (chunk/write-double! c 1 7.0)
   (chunk/col-persistent! c)
   c)))


(def v25_l117 (def c-snapshot (chunk/col-fork c-base)))


(def
 v27_l120
 (let
  [c (chunk/col-transient c-base)]
  (chunk/write-double! c 0 999.0)
  (chunk/col-persistent! c)))


(def
 v28_l124
 {:base-mutated (chunk/read-double c-base 0),
  :snapshot-safe (chunk/read-double c-snapshot 0),
  :snapshot-v1 (chunk/read-double c-snapshot 1)})


(deftest
 t29_l128
 (is
  ((fn
    [{:keys [base-mutated snapshot-safe snapshot-v1]}]
    (and
     (= 999.0 base-mutated)
     (= 42.0 snapshot-safe)
     (= 7.0 snapshot-v1)))
   v28_l124)))


(def
 v31_l136
 (let
  [c1
   (let
    [c (chunk/col-transient (chunk/make-chunk :int64 10))]
    (chunk/write-long! c 0 42)
    (chunk/col-persistent! c)
    c)
   c2
   (chunk/col-fork c1)
   _
   (let
    [c (chunk/col-transient c1)]
    (chunk/write-long! c 0 999)
    (chunk/col-persistent! c))]
  {:original (chunk/read-long c1 0), :fork (chunk/read-long c2 0)}))


(deftest
 t32_l147
 (is
  ((fn [{:keys [original fork]}] (and (= 999 original) (= 42 fork)))
   v31_l136)))


(def
 v34_l154
 (def c-seq (chunk/chunk-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])))


(def
 v35_l156
 [(chunk/chunk-length c-seq)
  (chunk/read-double c-seq 0)
  (chunk/read-double c-seq 2)
  (chunk/read-double c-seq 4)])


(deftest
 t36_l161
 (is
  ((fn
    [[len v0 v2 v4]]
    (and (= 5 len) (= 1.0 v0) (= 3.0 v2) (= 5.0 v4)))
   v35_l156)))


(def v38_l177 (def idx-empty (index/make-index :float64)))


(def v39_l179 (index/idx-length idx-empty))


(deftest t40_l181 (is ((fn [n] (= 0 n)) v39_l179)))


(def v41_l184 (def idx-100 (index/index-from-seq :float64 (range 100))))


(def
 v42_l186
 [(index/idx-length idx-100)
  (index/idx-get-double idx-100 0)
  (index/idx-get-double idx-100 50)
  (index/idx-get-double idx-100 99)])


(deftest
 t43_l191
 (is
  ((fn
    [[len v0 v50 v99]]
    (and (= 100 len) (= 0.0 v0) (= 50.0 v50) (= 99.0 v99)))
   v42_l186)))


(def
 v45_l201
 (def idx-10k (index/index-from-seq :float64 (range 10000))))


(def
 v46_l203
 [(index/idx-length idx-10k)
  (index/idx-get-double idx-10k 0)
  (index/idx-get-double idx-10k 5000)
  (index/idx-get-double idx-10k 9999)])


(deftest
 t47_l208
 (is
  ((fn
    [[len v0 v5k v9999]]
    (and (= 10000 len) (= 0.0 v0) (= 5000.0 v5k) (= 9999.0 v9999)))
   v46_l203)))


(def
 v49_l215
 (try
  (index/idx-get-double idx-100 -1)
  :no-error
  (catch IndexOutOfBoundsException _ :threw)))


(deftest t50_l220 (is ((fn [r] (= :threw r)) v49_l215)))


(def
 v51_l223
 (try
  (index/idx-get-double idx-100 100)
  :no-error
  (catch IndexOutOfBoundsException _ :threw)))


(deftest t52_l228 (is ((fn [r] (= :threw r)) v51_l223)))


(def
 v54_l233
 (def idx-original (index/index-from-seq :float64 (range 100))))


(def
 v55_l235
 (def
  idx-modified
  (->
   (index/idx-fork idx-original)
   index/idx-transient
   (index/idx-set! 0 999.0)
   index/idx-persistent!)))


(def
 v56_l241
 {:original (index/idx-get-double idx-original 0),
  :modified (index/idx-get-double idx-modified 0),
  :shared (index/idx-get-double idx-modified 50)})


(deftest
 t57_l245
 (is
  ((fn
    [{:keys [original modified shared]}]
    (and (= 0.0 original) (= 999.0 modified) (= 50.0 shared)))
   v56_l241)))


(def
 v59_l253
 (def
  idx-appended
  (->
   (index/make-index :float64)
   index/idx-transient
   (index/idx-append! 1.0)
   (index/idx-append! 2.0)
   (index/idx-append! 3.0)
   index/idx-persistent!)))


(def
 v60_l261
 [(index/idx-length idx-appended)
  (index/idx-get-double idx-appended 0)
  (index/idx-get-double idx-appended 1)
  (index/idx-get-double idx-appended 2)])


(deftest
 t61_l266
 (is
  ((fn
    [[len v0 v1 v2]]
    (and (= 3 len) (= 1.0 v0) (= 2.0 v1) (= 3.0 v2)))
   v60_l261)))


(def
 v63_l273
 (try
  (index/idx-set! idx-100 0 42.0)
  :no-error
  (catch IllegalStateException _ :threw)))


(deftest t64_l278 (is ((fn [r] (= :threw r)) v63_l273)))


(def
 v66_l286
 (let
  [idx (index/idx-transient (index/make-index :float64))]
  (assert (empty? (index/idx-dirty-chunks idx)))
  (index/idx-append! idx 42.0)
  (let
   [dirty-after-append (index/idx-dirty-chunks idx)]
   (index/idx-clear-dirty! idx)
   (let
    [dirty-after-clear (index/idx-dirty-chunks idx)]
    (index/idx-set! idx 0 99.0)
    {:after-append (contains? dirty-after-append [0]),
     :after-clear (empty? dirty-after-clear),
     :after-set (contains? (index/idx-dirty-chunks idx) [0])}))))


(deftest
 t67_l301
 (is
  ((fn
    [{:keys [after-append after-clear after-set]}]
    (and after-append after-clear after-set))
   v66_l286)))


(def
 v69_l313
 (def stats-idx (index/index-from-seq :float64 (range 100))))


(def v70_l315 (index/idx-stats stats-idx))


(deftest
 t71_l317
 (is
  ((fn
    [{:keys [count sum min-val max-val sum-sq]}]
    (and
     (= 100 count)
     (= 4950.0 sum)
     (= 0.0 min-val)
     (= 99.0 max-val)
     (= 328350.0 sum-sq)))
   v70_l315)))


(def
 v73_l327
 {:sum (index/idx-sum-stats stats-idx),
  :mean (index/idx-mean-stats stats-idx),
  :min (index/idx-min-stats stats-idx),
  :max (index/idx-max-stats stats-idx)})


(deftest
 t74_l332
 (is
  ((fn
    [{:keys [sum mean min max]}]
    (and (= 4950.0 sum) (= 49.5 mean) (= 0.0 min) (= 99.0 max)))
   v73_l327)))


(def
 v76_l341
 (let
  [v
   (index/idx-variance-stats stats-idx)
   s
   (index/idx-stddev-stats stats-idx)]
  {:variance (number? v), :stddev (number? s)}))


(deftest
 t77_l346
 (is ((fn [{:keys [variance stddev]}] (and variance stddev)) v76_l341)))


(def
 v79_l356
 (let
  [idx (index/idx-transient (index/make-index :float64))]
  (dotimes [i 100] (index/idx-append! idx (double i)))
  (let
   [s (index/idx-stats (index/idx-persistent! idx))]
   {:count (:count s),
    :sum (:sum s),
    :min (:min-val s),
    :max (:max-val s)})))


(deftest
 t80_l362
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and (= 100 count) (== 4950.0 sum) (== 0.0 min) (== 99.0 max)))
   v79_l356)))


(def
 v82_l371
 (let
  [idx (index/idx-transient (index/make-index :float64)) n 10000]
  (dotimes [i n] (index/idx-append! idx (double i)))
  (let
   [s (index/idx-stats (index/idx-persistent! idx))]
   {:count (:count s),
    :sum (:sum s),
    :min (:min-val s),
    :max (:max-val s)})))


(deftest
 t83_l378
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and
     (= 10000 count)
     (== 4.9995E7 sum)
     (== 0.0 min)
     (== 9999.0 max)))
   v82_l371)))


(def
 v85_l387
 (let
  [idx (index/idx-transient (index/make-index :int64))]
  (dotimes [i 200] (index/idx-append! idx (long i)))
  (let
   [s (index/idx-stats (index/idx-persistent! idx))]
   {:count (:count s), :sum (:sum s)})))


(deftest
 t86_l392
 (is
  ((fn [{:keys [count sum]}] (and (= 200 count) (== 19900.0 sum)))
   v85_l387)))


(def
 v88_l404
 (def
  mc-idx
  (index/index-from-seq :float64 (range 1000) {:chunk-size 100})))


(def v89_l407 (count (index/idx-all-chunk-stats mc-idx)))


(deftest t90_l409 (is ((fn [n] (= 10 n)) v89_l407)))


(def
 v91_l412
 (let
  [s (index/idx-stats mc-idx)]
  {:count (:count s),
   :sum (:sum s),
   :min (:min-val s),
   :max (:max-val s)}))


(deftest
 t92_l416
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and (= 1000 count) (= 499500.0 sum) (= 0.0 min) (= 999.0 max)))
   v91_l412)))


(def v94_l433 (def gt-result (index/idx-filter-gt mc-idx 850.0)))


(def
 v95_l435
 {:count (count gt-result),
  :first (first gt-result),
  :last (last gt-result)})


(deftest
 t96_l439
 (is
  ((fn
    [{:keys [count first last]}]
    (and (= 149 count) (= 851 first) (= 999 last)))
   v95_l435)))


(def v98_l445 (def lt-result (index/idx-filter-lt mc-idx 50.0)))


(def
 v99_l447
 {:count (count lt-result),
  :first (first lt-result),
  :last (last lt-result)})


(deftest
 t100_l451
 (is
  ((fn
    [{:keys [count first last]}]
    (and (= 50 count) (= 0 first) (= 49 last)))
   v99_l447)))


(def
 v102_l457
 (def range-result (index/idx-filter-range mc-idx 400.0 600.0)))


(def
 v103_l459
 {:count (count range-result),
  :first (first range-result),
  :last (last range-result)})


(deftest
 t104_l463
 (is
  ((fn
    [{:keys [count first last]}]
    (and (= 200 count) (= 400 first) (= 599 last)))
   v103_l459)))


(def
 v106_l477
 {:sum (index/idx-sum-range mc-idx 200.0 400.0),
  :count (index/idx-count-range mc-idx 200.0 400.0),
  :mean (index/idx-mean-range mc-idx 200.0 400.0),
  :min (index/idx-min-range mc-idx 200.0 400.0),
  :max (index/idx-max-range mc-idx 200.0 400.0)})


(deftest
 t107_l483
 (is
  ((fn
    [{:keys [sum count mean min max]}]
    (and
     (= 59900.0 sum)
     (= 200 count)
     (= 299.5 mean)
     (= 200.0 min)
     (= 399.0 max)))
   v106_l477)))


(def
 v109_l493
 {:count (index/idx-count-range mc-idx 150.0 250.0),
  :sum (index/idx-sum-range mc-idx 150.0 250.0)})


(deftest
 t110_l496
 (is
  ((fn [{:keys [count sum]}] (and (= 100 count) (= 19950.0 sum)))
   v109_l493)))


(def
 v112_l503
 {:count (index/idx-count-range mc-idx 2000.0 3000.0),
  :sum (index/idx-sum-range mc-idx 2000.0 3000.0)})


(deftest
 t113_l506
 (is
  ((fn [{:keys [count sum]}] (and (= 0 count) (= 0.0 sum))) v112_l503)))


(def
 v115_l518
 (def big-idx (index/index-from-seq :float64 (range 25000))))


(def
 v116_l520
 (let
  [s (index/idx-stats big-idx)]
  {:count (:count s),
   :sum (:sum s),
   :min (:min-val s),
   :max (:max-val s)}))


(deftest
 t117_l526
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and
     (= 25000 count)
     (= 3.124875E8 sum)
     (= 0.0 min)
     (= 24999.0 max)))
   v116_l520)))


(def
 v119_l535
 (let
  [s (index/idx-stats-range big-idx 0 8192)]
  {:count (:count s),
   :sum (:sum s),
   :min (:min-val s),
   :max (:max-val s)}))


(deftest
 t120_l541
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and
     (= 8192 count)
     (= 3.3550336E7 sum)
     (= 0.0 min)
     (= 8191.0 max)))
   v119_l535)))


(def
 v122_l550
 (let
  [s (index/idx-stats-range big-idx 4000 20000)]
  {:count (:count s),
   :sum (:sum s),
   :min (:min-val s),
   :max (:max-val s)}))


(deftest
 t123_l556
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and
     (= 16000 count)
     (= 1.91992E8 sum)
     (= 4000.0 min)
     (= 19999.0 max)))
   v122_l550)))


(def
 v125_l565
 (let
  [range-s
   (index/idx-stats-range big-idx 0 25000)
   full-s
   (index/idx-stats big-idx)]
  {:count-match? (= (:count full-s) (:count range-s)),
   :sum-match? (= (:sum full-s) (:sum range-s)),
   :min-match? (= (:min-val full-s) (:min-val range-s)),
   :max-match? (= (:max-val full-s) (:max-val range-s))}))


(deftest t126_l572 (is ((fn [m] (every? true? (vals m))) v125_l565)))


(def
 v128_l577
 (let
  [s (index/idx-stats-range big-idx 100 101)]
  {:count (:count s), :sum (:sum s)}))


(deftest
 t129_l580
 (is
  ((fn [{:keys [count sum]}] (and (= 1 count) (= 100.0 sum)))
   v128_l577)))


(def
 v130_l584
 (let
  [s (index/idx-stats-range big-idx 0 10)]
  {:count (:count s), :sum (:sum s)}))


(deftest
 t131_l587
 (is
  ((fn [{:keys [count sum]}] (and (= 10 count) (= 45.0 sum)))
   v130_l584)))


(def
 v132_l591
 (let
  [s (index/idx-stats-range big-idx 24990 25000)]
  {:count (:count s),
   :sum (:sum s),
   :min (:min-val s),
   :max (:max-val s)}))


(deftest
 t133_l595
 (is
  ((fn
    [{:keys [count sum min max]}]
    (and
     (= 10 count)
     (= 249945.0 sum)
     (= 24990.0 min)
     (= 24999.0 max)))
   v132_l591)))


(def
 v135_l604
 (try
  (index/idx-stats-range big-idx 100 100)
  :no-error
  (catch Exception _ :threw)))


(deftest t136_l607 (is ((fn [r] (= :threw r)) v135_l604)))


(def
 v137_l610
 (try
  (index/idx-stats-range big-idx 100 99)
  :no-error
  (catch Exception _ :threw)))


(deftest t138_l613 (is ((fn [r] (= :threw r)) v137_l610)))


(def
 v139_l616
 (try
  (index/idx-stats-range big-idx -1 100)
  :no-error
  (catch Exception _ :threw)))


(deftest t140_l619 (is ((fn [r] (= :threw r)) v139_l616)))
