package stratum.internal;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * Analytics operations — JIT-isolated from ColumnOpsExt.
 *
 * Contains:
 * 1. T-Digest approximate quantile computation
 * 2. Isolation Forest training and scoring
 * 3. Window function support (argsort, row_number, lag/lead, running_sum)
 * 4. Top-N heap-based selection
 *
 * These operations are not on the critical SIMD/group-by hot paths.
 * Separating them prevents bytecode bloat from degrading JIT compilation
 * of the core paths in ColumnOps and ColumnOpsExt.
 */
public final class ColumnOpsAnalytics {

    // Re-use ColumnOps pool and thresholds
    private static ForkJoinPool POOL = ColumnOps.POOL;

    // Configurable morsel size for isolation forest scoring.
    // L1-resident (512 rows × 5 features × 8B = 20KB) gives best per-row latency.
    // Larger morsels (65536) reduce morsel overhead but spill to L2.
    private static volatile int IF_MORSEL_SIZE = 512;

    /** Set isolation forest morsel size (for tuning). */
    public static void setIForestMorselSize(int size) { IF_MORSEL_SIZE = size; }

    // =========================================================================
    // T-Digest approximate quantile
    // =========================================================================

    /**
     * K_1 scale function: maps quantile q ∈ [0,1] to scale space.
     * k(q) = (delta / (2*PI)) * asin(2*q - 1)
     */
    private static double k1(double q, double delta) {
        return (delta / (2.0 * Math.PI)) * Math.asin(2.0 * q - 1.0);
    }

    /**
     * Weight limit for K_1 scaling using the derivative-based formula.
     * max_weight(q) = totalWeight * 2 * sin(PI/delta) * sqrt(q * (1-q))
     * Uses precomputed sinFactor = 2 * sin(PI / delta) to avoid trig per centroid.
     * Cost: 1 sqrt per call vs asin+sin in the old k1Limit.
     */
    private static double k1WeightLimit(double q, double totalWeight, double sinFactor) {
        return Math.max(1.0, totalWeight * sinFactor * Math.sqrt(q * (1.0 - q)));
    }

    /**
     * Create a new t-digest. Returns [means, weights, tempMeans, tempWeights].
     * @param compression delta parameter (100-300 typical, higher = more accurate)
     * @return Object[] { double[] means, double[] weights, double[] tempMeans, double[] tempWeights }
     */
    public static Object[] tdigestCreate(double compression) {
        int maxCentroids = (int)(compression * 2) + 10;
        int bufSize = maxCentroids * 5;
        return new Object[] {
            new double[maxCentroids],  // means
            new double[maxCentroids],  // weights
            new double[bufSize],       // tempMeans
            new double[bufSize]        // tempWeights
        };
    }

    /**
     * Sort two parallel arrays by key (quicksort with median-of-3).
     */
    private static void sortPairs(double[] keys, double[] vals, int lo, int hi) {
        if (hi - lo <= 16) {
            // Insertion sort for small ranges
            for (int i = lo + 1; i < hi; i++) {
                double k = keys[i], v = vals[i];
                int j = i - 1;
                while (j >= lo && keys[j] > k) {
                    keys[j + 1] = keys[j];
                    vals[j + 1] = vals[j];
                    j--;
                }
                keys[j + 1] = k;
                vals[j + 1] = v;
            }
            return;
        }
        int mid = lo + (hi - lo) / 2;
        // Median-of-three pivot
        if (keys[lo] > keys[mid]) { double t = keys[lo]; keys[lo] = keys[mid]; keys[mid] = t; t = vals[lo]; vals[lo] = vals[mid]; vals[mid] = t; }
        if (keys[lo] > keys[hi - 1]) { double t = keys[lo]; keys[lo] = keys[hi - 1]; keys[hi - 1] = t; t = vals[lo]; vals[lo] = vals[hi - 1]; vals[hi - 1] = t; }
        if (keys[mid] > keys[hi - 1]) { double t = keys[mid]; keys[mid] = keys[hi - 1]; keys[hi - 1] = t; t = vals[mid]; vals[mid] = vals[hi - 1]; vals[hi - 1] = t; }
        double pivot = keys[mid];
        double t = keys[mid]; keys[mid] = keys[hi - 2]; keys[hi - 2] = t;
        t = vals[mid]; vals[mid] = vals[hi - 2]; vals[hi - 2] = t;
        int i = lo, j = hi - 2;
        while (true) {
            while (keys[++i] < pivot) ;
            while (keys[--j] > pivot) ;
            if (i >= j) break;
            t = keys[i]; keys[i] = keys[j]; keys[j] = t;
            t = vals[i]; vals[i] = vals[j]; vals[j] = t;
        }
        t = keys[i]; keys[i] = keys[hi - 2]; keys[hi - 2] = t;
        t = vals[i]; vals[i] = vals[hi - 2]; vals[hi - 2] = t;
        sortPairs(keys, vals, lo, i);
        sortPairs(keys, vals, i + 1, hi);
    }

    /**
     * Compress a t-digest: merge temp buffer into centroids.
     * Uses merge-based approach: centroids are already sorted, sort temp buffer,
     * then merge the two sorted sequences. O(B log B + C + B) instead of O((B+C)^2).
     * Uses weight-limit formula (1 sqrt) instead of k1Limit (asin+sin) per centroid.
     */
    public static void tdigestCompress(double[] means, double[] weights, int[] centroidCount,
                                        double[] tempMeans, double[] tempWeights, int tempCount,
                                        double totalWeight, double compression) {
        int cc = centroidCount[0];
        int total = cc + tempCount;
        if (total == 0) return;

        // Precompute K_1 weight limit constant: 2 * sin(PI / delta)
        double sinFactor = 2.0 * Math.sin(Math.PI / compression);

        // Sort the temp buffer by mean (O(B log B))
        if (tempCount > 1) {
            sortPairs(tempMeans, tempWeights, 0, tempCount);
        }

        // Merge sorted centroids [0..cc) with sorted temp [0..tempCount) into allMeans/allWeights
        double[] allMeans = new double[total];
        double[] allWeights = new double[total];
        int a = 0, b = 0, out = 0;
        while (a < cc && b < tempCount) {
            if (means[a] <= tempMeans[b]) {
                allMeans[out] = means[a]; allWeights[out] = weights[a]; a++;
            } else {
                allMeans[out] = tempMeans[b]; allWeights[out] = tempWeights[b]; b++;
            }
            out++;
        }
        while (a < cc) { allMeans[out] = means[a]; allWeights[out] = weights[a]; a++; out++; }
        while (b < tempCount) { allMeans[out] = tempMeans[b]; allWeights[out] = tempWeights[b]; b++; out++; }

        // Merge centroids respecting weight limits
        means[0] = allMeans[0];
        weights[0] = allWeights[0];
        int newCC = 1;
        double weightSoFar = allWeights[0];

        for (int ii = 1; ii < total; ii++) {
            double q = weightSoFar / totalWeight;
            double limit = k1WeightLimit(q, totalWeight, sinFactor);

            if (weights[newCC - 1] + allWeights[ii] <= limit) {
                double combinedWeight = weights[newCC - 1] + allWeights[ii];
                means[newCC - 1] = (means[newCC - 1] * weights[newCC - 1] + allMeans[ii] * allWeights[ii]) / combinedWeight;
                weights[newCC - 1] = combinedWeight;
            } else {
                if (newCC < means.length) {
                    means[newCC] = allMeans[ii];
                    weights[newCC] = allWeights[ii];
                    newCC++;
                } else {
                    double combinedWeight = weights[newCC - 1] + allWeights[ii];
                    means[newCC - 1] = (means[newCC - 1] * weights[newCC - 1] + allMeans[ii] * allWeights[ii]) / combinedWeight;
                    weights[newCC - 1] = combinedWeight;
                }
            }
            weightSoFar += allWeights[ii];
        }
        centroidCount[0] = newCC;
    }

    /**
     * Query a t-digest for a quantile value.
     */
    public static double tdigestQuantile(double[] means, double[] weights, int centroidCount,
                                          double totalWeight, double min, double max, double q) {
        if (centroidCount == 0) return Double.NaN;
        if (q <= 0.0) return min;
        if (q >= 1.0) return max;
        if (centroidCount == 1) return means[0];

        double target = q * totalWeight;
        double cumWeight = 0;

        for (int i = 0; i < centroidCount; i++) {
            double halfW = weights[i] / 2.0;
            if (cumWeight + halfW >= target) {
                if (i == 0) {
                    double innerTarget = target;
                    double innerRange = halfW;
                    if (innerRange <= 0) return means[0];
                    double frac = innerTarget / innerRange;
                    return min + frac * (means[0] - min);
                }
                double prevMean = means[i - 1];
                double prevHalfW = weights[i - 1] / 2.0;
                double gap = cumWeight - prevHalfW;
                double target2 = target - gap;
                double range2 = (halfW + prevHalfW);
                if (range2 <= 0) return means[i];
                double frac = target2 / range2;
                return prevMean + frac * (means[i] - prevMean);
            }
            cumWeight += weights[i];
        }
        double lastHalfW = weights[centroidCount - 1] / 2.0;
        double remaining = totalWeight - cumWeight + lastHalfW;
        if (remaining <= 0) return max;
        double frac = (target - (cumWeight - lastHalfW)) / remaining;
        return means[centroidCount - 1] + frac * (max - means[centroidCount - 1]);
    }

    /**
     * Build a t-digest from a double array using incremental buffer compression.
     */
    public static Object[] tdigestBuild(double[] values, int n, double compression) {
        if (n == 0) {
            return new Object[] { new double[0], new double[0], 0, 0.0,
                                  Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
        }

        int maxCentroids = (int)(compression * 2) + 10;
        int bufSize = maxCentroids * 15;
        double[] means = new double[maxCentroids];
        double[] weights = new double[maxCentroids];
        double[] buffer = new double[bufSize];
        int[] centroidCount = new int[] { 0 };
        int bufCount = 0;
        double totalWeight = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        double[] mergeMeans = new double[maxCentroids + bufSize];
        double[] mergeWeights = new double[maxCentroids + bufSize];

        double sinFactor = 2.0 * Math.sin(Math.PI / compression);

        for (int i = 0; i < n; i++) {
            double v = values[i];
            if (Double.isNaN(v)) continue;
            if (v < min) min = v;
            if (v > max) max = v;
            buffer[bufCount++] = v;
            totalWeight += 1.0;

            if (bufCount >= bufSize) {
                compressBatch(means, weights, centroidCount, buffer, bufCount,
                              totalWeight, sinFactor, mergeMeans, mergeWeights);
                bufCount = 0;
            }
        }

        if (bufCount > 0) {
            compressBatch(means, weights, centroidCount, buffer, bufCount,
                          totalWeight, sinFactor, mergeMeans, mergeWeights);
        }

        return new Object[] { means, weights, centroidCount[0], totalWeight, min, max };
    }

    /**
     * Compress batch of unit-weight values into centroid digest.
     */
    private static void compressBatch(double[] means, double[] weights, int[] centroidCount,
                                       double[] buffer, int bufCount,
                                       double totalWeight, double sinFactor,
                                       double[] mergeMeans, double[] mergeWeights) {
        int cc = centroidCount[0];

        Arrays.sort(buffer, 0, bufCount);

        int total = cc + bufCount;
        int a = 0, b = 0, out = 0;
        while (a < cc && b < bufCount) {
            if (means[a] <= buffer[b]) {
                mergeMeans[out] = means[a]; mergeWeights[out] = weights[a]; a++;
            } else {
                mergeMeans[out] = buffer[b]; mergeWeights[out] = 1.0; b++;
            }
            out++;
        }
        while (a < cc) { mergeMeans[out] = means[a]; mergeWeights[out] = weights[a]; a++; out++; }
        while (b < bufCount) { mergeMeans[out] = buffer[b]; mergeWeights[out] = 1.0; b++; out++; }

        means[0] = mergeMeans[0];
        weights[0] = mergeWeights[0];
        int newCC = 1;
        double weightSoFar = mergeWeights[0];

        for (int i = 1; i < total; i++) {
            double q = weightSoFar / totalWeight;
            double limit = k1WeightLimit(q, totalWeight, sinFactor);

            if (weights[newCC - 1] + mergeWeights[i] <= limit) {
                double cw = weights[newCC - 1] + mergeWeights[i];
                means[newCC - 1] = (means[newCC - 1] * weights[newCC - 1] + mergeMeans[i] * mergeWeights[i]) / cw;
                weights[newCC - 1] = cw;
            } else if (newCC < means.length) {
                means[newCC] = mergeMeans[i];
                weights[newCC] = mergeWeights[i];
                newCC++;
            } else {
                double cw = weights[newCC - 1] + mergeWeights[i];
                means[newCC - 1] = (means[newCC - 1] * weights[newCC - 1] + mergeMeans[i] * mergeWeights[i]) / cw;
                weights[newCC - 1] = cw;
            }
            weightSoFar += mergeWeights[i];
        }
        centroidCount[0] = newCC;
    }

    /**
     * Build a t-digest and immediately query a quantile.
     */
    public static double tdigestApproxQuantile(double[] values, int n, double quantile, double compression) {
        Object[] digest = tdigestBuild(values, n, compression);
        double[] means = (double[]) digest[0];
        double[] weights = (double[]) digest[1];
        int cc = (Integer) digest[2];
        double totalWeight = (Double) digest[3];
        double min = (Double) digest[4];
        double max = (Double) digest[5];
        return tdigestQuantile(means, weights, cc, totalWeight, min, max, quantile);
    }

    /**
     * Merge two t-digests into the first one.
     */
    public static double tdigestMerge(double[] means1, double[] weights1, int[] cc1,
                                       double totalWeight1,
                                       double[] means2, double[] weights2, int cc2,
                                       double compression) {
        double newTotal = totalWeight1;
        for (int i = 0; i < cc2; i++) {
            newTotal += weights2[i];
        }
        tdigestCompress(means1, weights1, cc1, means2, weights2, cc2, newTotal, compression);
        return newTotal;
    }

    /**
     * Build a t-digest from a subrange of a double array.
     */
    private static Object[] tdigestBuildRange(double[] values, int start, int end, double compression) {
        int n = end - start;
        if (n == 0) {
            return new Object[] { new double[0], new double[0], 0, 0.0,
                                  Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
        }

        int maxCentroids = (int)(compression * 2) + 10;
        int bufSize = maxCentroids * 15;
        double[] means = new double[maxCentroids];
        double[] weights = new double[maxCentroids];
        double[] buffer = new double[bufSize];
        int[] centroidCount = new int[] { 0 };
        int bufCount = 0;
        double totalWeight = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        double[] mergeMeans = new double[maxCentroids + bufSize];
        double[] mergeWeights = new double[maxCentroids + bufSize];
        double sinFactor = 2.0 * Math.sin(Math.PI / compression);

        for (int i = start; i < end; i++) {
            double v = values[i];
            if (Double.isNaN(v)) continue;
            if (v < min) min = v;
            if (v > max) max = v;
            buffer[bufCount++] = v;
            totalWeight += 1.0;

            if (bufCount >= bufSize) {
                compressBatch(means, weights, centroidCount, buffer, bufCount,
                              totalWeight, sinFactor, mergeMeans, mergeWeights);
                bufCount = 0;
            }
        }

        if (bufCount > 0) {
            compressBatch(means, weights, centroidCount, buffer, bufCount,
                          totalWeight, sinFactor, mergeMeans, mergeWeights);
        }

        return new Object[] { means, weights, centroidCount[0], totalWeight, min, max };
    }

    /**
     * Parallel approximate quantile via per-thread t-digests + merge.
     */
    @SuppressWarnings("unchecked")
    public static double tdigestApproxQuantileParallel(double[] values, int n, double quantile, double compression) {
        if (n < ColumnOps.PARALLEL_THRESHOLD) {
            return tdigestApproxQuantile(values, n, quantile, compression);
        }

        int nThreads = POOL.getParallelism();
        int chunkSize = (n + nThreads - 1) / nThreads;

        java.util.concurrent.Future<Object[]>[] futures = new java.util.concurrent.Future[nThreads];
        int actualThreads = 0;
        for (int t = 0; t < nThreads; t++) {
            int start = t * chunkSize;
            int end = Math.min(start + chunkSize, n);
            if (start >= end) break;
            actualThreads++;
            final int s = start, e = end;
            futures[t] = POOL.submit(() -> tdigestBuildRange(values, s, e, compression));
        }

        Object[][] digests = new Object[actualThreads][];
        for (int t = 0; t < actualThreads; t++) {
            try {
                digests[t] = futures[t].get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        double[] means = (double[]) digests[0][0];
        double[] weights = (double[]) digests[0][1];
        int[] cc = new int[] { (Integer) digests[0][2] };
        double totalWeight = (Double) digests[0][3];
        double min = (Double) digests[0][4];
        double max = (Double) digests[0][5];

        for (int t = 1; t < actualThreads; t++) {
            double[] m2 = (double[]) digests[t][0];
            double[] w2 = (double[]) digests[t][1];
            int cc2 = (Integer) digests[t][2];
            double min2 = (Double) digests[t][4];
            double max2 = (Double) digests[t][5];

            if (cc2 > 0) {
                totalWeight = tdigestMerge(means, weights, cc, totalWeight, m2, w2, cc2, compression);
                if (min2 < min) min = min2;
                if (max2 > max) max = max2;
            }
        }

        return tdigestQuantile(means, weights, cc[0], totalWeight, min, max, quantile);
    }

    // =========================================================================
    // Isolation Forest (Liu et al. 2008) — Integer-Packed Layout
    // =========================================================================

    private static final double EULER = 0.5772156649;

    /** Pack feature index + split value into a single long. */
    private static long packNode(int feat, double splitVal) {
        return ((long) feat << 32) | (Integer.toUnsignedLong(Float.floatToRawIntBits((float) splitVal)));
    }

    /**
     * Expected path length of unsuccessful BST search in a tree of n nodes.
     * c(n) = 2*H(n-1) - 2*(n-1)/n where H(i) = ln(i) + Euler constant.
     */
    public static double expectedPathLength(int n) {
        if (n <= 1) return 0.0;
        if (n == 2) return 1.0;
        double h = Math.log(n - 1.0) + EULER;
        return 2.0 * h - 2.0 * (n - 1.0) / n;
    }

    /**
     * Train an isolation forest. Returns packed long[] forest.
     */
    public static long[] iforestTrain(double[][] features, int nRows, int nFeatures,
                                       int nTrees, int sampleSize, long seed) {
        int maxNodes = 2 * sampleSize - 1;
        long[] forest = new long[nTrees * maxNodes];
        int maxDepth = (int) Math.ceil(Math.log(sampleSize) / Math.log(2));
        java.util.Random rng = new java.util.Random(seed);

        double[] eplCache = new double[sampleSize + 1];
        for (int i = 0; i <= sampleSize; i++) eplCache[i] = expectedPathLength(i);

        int[] featArr = new int[maxNodes];
        double[] splitArr = new double[maxNodes];
        int[] sizeArr = new int[maxNodes];

        for (int t = 0; t < nTrees; t++) {
            int treeOffset = t * maxNodes;

            int[] sampleIndices = new int[sampleSize];
            for (int i = 0; i < sampleSize; i++) {
                sampleIndices[i] = rng.nextInt(nRows);
            }

            double[][] sample = new double[nFeatures][sampleSize];
            for (int f = 0; f < nFeatures; f++) {
                for (int i = 0; i < sampleSize; i++) {
                    sample[f][i] = features[f][sampleIndices[i]];
                }
            }

            for (int nd = 0; nd < maxNodes; nd++) {
                featArr[nd] = -1;
                splitArr[nd] = 0;
                sizeArr[nd] = 0;
            }

            int[] indices = new int[sampleSize];
            for (int i = 0; i < sampleSize; i++) indices[i] = i;

            int[][] stack = new int[maxNodes][4];
            int stackTop = 0;
            stack[stackTop++] = new int[]{0, 0, sampleSize, 0};

            while (stackTop > 0) {
                int[] entry = stack[--stackTop];
                int node = entry[0], start = entry[1], end = entry[2], depth = entry[3];
                int size = end - start;

                if (size <= 1 || depth >= maxDepth || node >= maxNodes) {
                    featArr[node] = -1;
                    sizeArr[node] = size;
                    continue;
                }

                int feat = rng.nextInt(nFeatures);
                double minVal = Double.POSITIVE_INFINITY, maxVal = Double.NEGATIVE_INFINITY;
                for (int i = start; i < end; i++) {
                    double v = sample[feat][indices[i]];
                    if (v < minVal) minVal = v;
                    if (v > maxVal) maxVal = v;
                }

                if (minVal >= maxVal) {
                    featArr[node] = -1;
                    sizeArr[node] = size;
                    continue;
                }

                double splitVal = minVal + rng.nextDouble() * (maxVal - minVal);
                featArr[node] = feat;
                splitArr[node] = splitVal;
                sizeArr[node] = size;

                int left = start, right = end - 1;
                while (left <= right) {
                    if (sample[feat][indices[left]] < splitVal) {
                        left++;
                    } else {
                        int tmp = indices[left]; indices[left] = indices[right]; indices[right] = tmp;
                        right--;
                    }
                }
                int lc = 2 * node + 1, rc = 2 * node + 2;
                if (lc < maxNodes && left > start) stack[stackTop++] = new int[]{lc, start, left, depth + 1};
                if (rc < maxNodes && left < end) stack[stackTop++] = new int[]{rc, left, end, depth + 1};
            }

            // Pad tree + compute path lengths via BFS, then pack into long[]
            int[] bfsQueue = new int[maxNodes];
            int[] bfsDepth = new int[maxNodes];
            double[] pathLen = new double[maxNodes];
            boolean[] isLeaf = new boolean[maxNodes];
            int bfsHead = 0, bfsTail = 0;
            bfsQueue[bfsTail] = 0; bfsDepth[bfsTail] = 0; bfsTail++;

            while (bfsHead < bfsTail) {
                int nd = bfsQueue[bfsHead];
                int dep = bfsDepth[bfsHead];
                bfsHead++;

                if (dep >= maxDepth || nd >= maxNodes) continue;

                if (featArr[nd] < 0) {
                    int ls = sizeArr[nd];
                    double pl = dep + eplCache[Math.min(ls, sampleSize)];

                    featArr[nd] = 0;
                    splitArr[nd] = Double.NEGATIVE_INFINITY;

                    int lc = 2 * nd + 1, rc = 2 * nd + 2;
                    if (lc < maxNodes) {
                        featArr[lc] = -2;
                        pathLen[lc] = pl;
                        bfsQueue[bfsTail] = lc; bfsDepth[bfsTail] = dep + 1; bfsTail++;
                    }
                    if (rc < maxNodes) {
                        featArr[rc] = -2;
                        pathLen[rc] = pl;
                        bfsQueue[bfsTail] = rc; bfsDepth[bfsTail] = dep + 1; bfsTail++;
                    }
                } else if (featArr[nd] == -2) {
                    double pl = pathLen[nd];
                    featArr[nd] = 0;
                    splitArr[nd] = Double.NEGATIVE_INFINITY;

                    int lc = 2 * nd + 1, rc = 2 * nd + 2;
                    if (lc < maxNodes) {
                        featArr[lc] = -2; pathLen[lc] = pl;
                        bfsQueue[bfsTail] = lc; bfsDepth[bfsTail] = dep + 1; bfsTail++;
                    }
                    if (rc < maxNodes) {
                        featArr[rc] = -2; pathLen[rc] = pl;
                        bfsQueue[bfsTail] = rc; bfsDepth[bfsTail] = dep + 1; bfsTail++;
                    }
                } else {
                    int lc = 2 * nd + 1, rc = 2 * nd + 2;
                    if (lc < maxNodes) { bfsQueue[bfsTail] = lc; bfsDepth[bfsTail] = dep + 1; bfsTail++; }
                    if (rc < maxNodes) { bfsQueue[bfsTail] = rc; bfsDepth[bfsTail] = dep + 1; bfsTail++; }
                }
            }

            // Phase 2: Fix maxDepth leaves and pack into long[]
            int firstLeaf = (1 << maxDepth) - 1;
            int lastLeaf = Math.min((1 << (maxDepth + 1)) - 2, maxNodes - 1);

            for (int nd = 0; nd < firstLeaf && nd < maxNodes; nd++) {
                forest[treeOffset + nd] = packNode(featArr[nd], splitArr[nd]);
            }

            for (int nd = firstLeaf; nd <= lastLeaf && nd < maxNodes; nd++) {
                double pl;
                if (featArr[nd] == -1) {
                    pl = maxDepth + eplCache[Math.min(sizeArr[nd], sampleSize)];
                } else if (featArr[nd] == -2) {
                    pl = pathLen[nd];
                } else {
                    pl = pathLen[nd];
                }
                forest[treeOffset + nd] = Double.doubleToRawLongBits(pl);
            }
        }
        return forest;
    }

    /**
     * Score rows using a trained isolation forest (single-threaded).
     */
    public static double[] iforestScore(long[] forest, int nTrees, int sampleSize,
                                         double[][] features, int nRows, int nFeatures) {
        return iforestScoreRange(forest, nTrees, sampleSize, features, nRows, nFeatures, 0, nRows);
    }

    private static void iforestScoreMorsel(long[] forest, int nTrees,
                                            double[] rowMajor, int nFeatures,
                                            double[] pathLengths, int morselStart, int morselLen,
                                            int treeSize, int maxDepth) {
        for (int t = 0; t < nTrees; t++) {
            final int treeOffset = t * treeSize;
            for (int i = 0; i < morselLen; i++) {
                final int base = i * nFeatures;
                int node = 0;
                for (int d = 0; d < maxDepth; d++) {
                    long packed = forest[treeOffset + node];
                    int feat = (int)(packed >>> 32);
                    float splitVal = Float.intBitsToFloat((int) packed);
                    int cmp = (rowMajor[base + feat] >= splitVal) ? 1 : 0;
                    node = 2 * node + 1 + cmp;
                }
                pathLengths[morselStart + i] += Double.longBitsToDouble(forest[treeOffset + node]);
            }
        }
    }

    private static void transposeMorsel(double[][] features, int nFeatures,
                                         double[] rowMajor, int morselStart, int morselLen) {
        for (int f = 0; f < nFeatures; f++) {
            final double[] col = features[f];
            for (int i = 0; i < morselLen; i++) {
                rowMajor[i * nFeatures + f] = col[morselStart + i];
            }
        }
    }

    private static double[] iforestScoreRange(long[] forest, int nTrees, int sampleSize,
                                               double[][] features, int nRows, int nFeatures,
                                               int start, int end) {
        int maxNodes = 2 * sampleSize - 1;
        int maxDepth = (int) Math.ceil(Math.log(sampleSize) / Math.log(2));
        double cPsi = expectedPathLength(sampleSize);
        double factor = Math.log(2.0) / (nTrees * cPsi);

        double[] pathLengths = new double[nRows];
        final int morsel = IF_MORSEL_SIZE;
        double[] rowMajor = new double[morsel * nFeatures];

        for (int ms = start; ms < end; ms += morsel) {
            int me = Math.min(ms + morsel, end);
            int morselLen = me - ms;
            transposeMorsel(features, nFeatures, rowMajor, ms, morselLen);
            iforestScoreMorsel(forest, nTrees, rowMajor, nFeatures,
                               pathLengths, ms, morselLen, maxNodes, maxDepth);
        }

        double[] scores = new double[nRows];
        for (int i = start; i < end; i++) {
            scores[i] = Math.exp(-pathLengths[i] * factor);
        }
        return scores;
    }

    /**
     * Score rows with morsel-driven parallelism.
     */
    public static double[] iforestScoreParallel(long[] forest, int nTrees, int sampleSize,
                                                 double[][] features, int nRows, int nFeatures) {
        if (nRows < ColumnOps.PARALLEL_THRESHOLD) {
            return iforestScore(forest, nTrees, sampleSize, features, nRows, nFeatures);
        }

        int maxNodes = 2 * sampleSize - 1;
        int maxDepth = (int) Math.ceil(Math.log(sampleSize) / Math.log(2));
        double cPsi = expectedPathLength(sampleSize);
        double factor = Math.log(2.0) / (nTrees * cPsi);

        double[] pathLengths = new double[nRows];
        double[] scores = new double[nRows];
        int nThreads = POOL.getParallelism() + 1;
        final int morsel = IF_MORSEL_SIZE;

        try {
            Future<?>[] futures = new Future<?>[nThreads];
            for (int th = 0; th < nThreads; th++) {
                final int threadStart = (int)((long) th * nRows / nThreads);
                final int threadEnd = (int)((long)(th + 1) * nRows / nThreads);
                futures[th] = POOL.submit(() -> {
                    double[] rowMajor = new double[morsel * nFeatures];
                    for (int ms = threadStart; ms < threadEnd; ms += morsel) {
                        int me = Math.min(ms + morsel, threadEnd);
                        int morselLen = me - ms;
                        transposeMorsel(features, nFeatures, rowMajor, ms, morselLen);
                        iforestScoreMorsel(forest, nTrees, rowMajor, nFeatures,
                                           pathLengths, ms, morselLen, maxNodes, maxDepth);
                    }
                    for (int i = threadStart; i < threadEnd; i++) {
                        scores[i] = Math.exp(-pathLengths[i] * factor);
                    }
                });
            }
            for (Future<?> f : futures) f.get();
        } catch (Exception e) {
            throw new RuntimeException("Parallel iforest scoring failed", e);
        }
        return scores;
    }

    // Morsel scoring that also tracks sum-of-squares for variance computation.
    private static void iforestScoreMorselVar(long[] forest, int nTrees,
                                               double[] rowMajor, int nFeatures,
                                               double[] pathLengths, double[] pathLengthsSq,
                                               int morselStart, int morselLen,
                                               int treeSize, int maxDepth) {
        for (int t = 0; t < nTrees; t++) {
            final int treeOffset = t * treeSize;
            for (int i = 0; i < morselLen; i++) {
                final int base = i * nFeatures;
                int node = 0;
                for (int d = 0; d < maxDepth; d++) {
                    long packed = forest[treeOffset + node];
                    int feat = (int)(packed >>> 32);
                    float splitVal = Float.intBitsToFloat((int) packed);
                    int cmp = (rowMajor[base + feat] >= splitVal) ? 1 : 0;
                    node = 2 * node + 1 + cmp;
                }
                double pl = Double.longBitsToDouble(forest[treeOffset + node]);
                pathLengths[morselStart + i] += pl;
                pathLengthsSq[morselStart + i] += pl * pl;
            }
        }
    }

    /**
     * Score rows and compute per-tree path length variance (parallel).
     * Returns double[2][nRows]: [0] = anomaly scores, [1] = path length variance.
     */
    public static double[][] iforestScoreAndVarianceParallel(long[] forest, int nTrees, int sampleSize,
                                                              double[][] features, int nRows, int nFeatures) {
        int maxNodes = 2 * sampleSize - 1;
        int maxDepth = (int) Math.ceil(Math.log(sampleSize) / Math.log(2));
        double cPsi = expectedPathLength(sampleSize);
        double factor = Math.log(2.0) / (nTrees * cPsi);
        double invTrees = 1.0 / nTrees;

        double[] pathLengths = new double[nRows];
        double[] pathLengthsSq = new double[nRows];
        double[] scores = new double[nRows];
        double[] variance = new double[nRows];
        final int morsel = IF_MORSEL_SIZE;

        if (nRows < ColumnOps.PARALLEL_THRESHOLD) {
            double[] rowMajor = new double[morsel * nFeatures];
            for (int ms = 0; ms < nRows; ms += morsel) {
                int me = Math.min(ms + morsel, nRows);
                int morselLen = me - ms;
                transposeMorsel(features, nFeatures, rowMajor, ms, morselLen);
                iforestScoreMorselVar(forest, nTrees, rowMajor, nFeatures,
                                      pathLengths, pathLengthsSq, ms, morselLen, maxNodes, maxDepth);
            }
            for (int i = 0; i < nRows; i++) {
                scores[i] = Math.exp(-pathLengths[i] * factor);
                double mean = pathLengths[i] * invTrees;
                variance[i] = pathLengthsSq[i] * invTrees - mean * mean;
            }
        } else {
            int nThreads = POOL.getParallelism() + 1;
            try {
                Future<?>[] futures = new Future<?>[nThreads];
                for (int th = 0; th < nThreads; th++) {
                    final int threadStart = (int)((long) th * nRows / nThreads);
                    final int threadEnd = (int)((long)(th + 1) * nRows / nThreads);
                    futures[th] = POOL.submit(() -> {
                        double[] rowMajor = new double[morsel * nFeatures];
                        for (int ms = threadStart; ms < threadEnd; ms += morsel) {
                            int me = Math.min(ms + morsel, threadEnd);
                            int morselLen = me - ms;
                            transposeMorsel(features, nFeatures, rowMajor, ms, morselLen);
                            iforestScoreMorselVar(forest, nTrees, rowMajor, nFeatures,
                                                  pathLengths, pathLengthsSq, ms, morselLen, maxNodes, maxDepth);
                        }
                        for (int i = threadStart; i < threadEnd; i++) {
                            scores[i] = Math.exp(-pathLengths[i] * factor);
                            double mean = pathLengths[i] * invTrees;
                            variance[i] = pathLengthsSq[i] * invTrees - mean * mean;
                        }
                    });
                }
                for (Future<?> f : futures) f.get();
            } catch (Exception e) {
                throw new RuntimeException("Parallel iforest scoring with variance failed", e);
            }
        }
        return new double[][] { scores, variance };
    }

    // =========================================================================
    // Top-N Optimization
    // =========================================================================

    /**
     * Returns the indices of the top-k elements from a double array, in sorted order.
     * Uses a bounded heap of size k for O(N log k) performance instead of O(N log N).
     */
    public static int[] topNIndices(double[] values, int length, int k, boolean ascending) {
        k = Math.min(k, length);
        if (k <= 0) return new int[0];

        int[] heapIdx = new int[k];
        double[] heapVal = new double[k];
        int heapSize = 0;

        for (int i = 0; i < length; i++) {
            double v = values[i];
            // Skip NaN values — they are NULL sentinels and should not participate in top-N
            if (v != v) continue;
            if (heapSize < k) {
                heapIdx[heapSize] = i;
                heapVal[heapSize] = v;
                heapSize++;
                int pos = heapSize - 1;
                while (pos > 0) {
                    int parent = (pos - 1) >>> 1;
                    boolean swap = ascending
                        ? heapVal[parent] < heapVal[pos]
                        : heapVal[parent] > heapVal[pos];
                    if (swap) {
                        double tv = heapVal[pos]; heapVal[pos] = heapVal[parent]; heapVal[parent] = tv;
                        int ti = heapIdx[pos]; heapIdx[pos] = heapIdx[parent]; heapIdx[parent] = ti;
                        pos = parent;
                    } else break;
                }
            } else {
                boolean replace = ascending
                    ? v < heapVal[0]
                    : v > heapVal[0];
                if (replace) {
                    heapIdx[0] = i;
                    heapVal[0] = v;
                    int pos = 0;
                    while (true) {
                        int left = 2 * pos + 1, right = left + 1, target = pos;
                        if (ascending) {
                            if (left < k && heapVal[left] > heapVal[target]) target = left;
                            if (right < k && heapVal[right] > heapVal[target]) target = right;
                        } else {
                            if (left < k && heapVal[left] < heapVal[target]) target = left;
                            if (right < k && heapVal[right] < heapVal[target]) target = right;
                        }
                        if (target == pos) break;
                        double tv = heapVal[pos]; heapVal[pos] = heapVal[target]; heapVal[target] = tv;
                        int ti = heapIdx[pos]; heapIdx[pos] = heapIdx[target]; heapIdx[target] = ti;
                        pos = target;
                    }
                }
            }
        }

        int[] result = new int[k];
        for (int i = k - 1; i >= 0; i--) {
            result[i] = heapIdx[0];
            heapSize--;
            if (heapSize > 0) {
                heapIdx[0] = heapIdx[heapSize];
                heapVal[0] = heapVal[heapSize];
                int pos = 0;
                while (true) {
                    int left = 2 * pos + 1, right = left + 1, target = pos;
                    if (ascending) {
                        if (left < heapSize && heapVal[left] > heapVal[target]) target = left;
                        if (right < heapSize && heapVal[right] > heapVal[target]) target = right;
                    } else {
                        if (left < heapSize && heapVal[left] < heapVal[target]) target = left;
                        if (right < heapSize && heapVal[right] < heapVal[target]) target = right;
                    }
                    if (target == pos) break;
                    double tv = heapVal[pos]; heapVal[pos] = heapVal[target]; heapVal[target] = tv;
                    int ti = heapIdx[pos]; heapIdx[pos] = heapIdx[target]; heapIdx[target] = ti;
                    pos = target;
                }
            }
        }
        return result;
    }

    /**
     * Returns the indices of the top-k elements from a long array, in sorted order.
     */
    public static int[] topNIndicesLong(long[] values, int length, int k, boolean ascending) {
        k = Math.min(k, length);
        if (k <= 0) return new int[0];

        int[] heapIdx = new int[k];
        long[] heapVal = new long[k];
        int heapSize = 0;

        for (int i = 0; i < length; i++) {
            long v = values[i];
            // Skip NULL sentinel (Long.MIN_VALUE)
            if (v == Long.MIN_VALUE) continue;
            if (heapSize < k) {
                heapIdx[heapSize] = i;
                heapVal[heapSize] = v;
                heapSize++;
                int pos = heapSize - 1;
                while (pos > 0) {
                    int parent = (pos - 1) >>> 1;
                    boolean swap = ascending
                        ? heapVal[parent] < heapVal[pos]
                        : heapVal[parent] > heapVal[pos];
                    if (swap) {
                        long tv = heapVal[pos]; heapVal[pos] = heapVal[parent]; heapVal[parent] = tv;
                        int ti = heapIdx[pos]; heapIdx[pos] = heapIdx[parent]; heapIdx[parent] = ti;
                        pos = parent;
                    } else break;
                }
            } else {
                boolean replace = ascending ? v < heapVal[0] : v > heapVal[0];
                if (replace) {
                    heapIdx[0] = i;
                    heapVal[0] = v;
                    int pos = 0;
                    while (true) {
                        int left = 2 * pos + 1, right = left + 1, target = pos;
                        if (ascending) {
                            if (left < k && heapVal[left] > heapVal[target]) target = left;
                            if (right < k && heapVal[right] > heapVal[target]) target = right;
                        } else {
                            if (left < k && heapVal[left] < heapVal[target]) target = left;
                            if (right < k && heapVal[right] < heapVal[target]) target = right;
                        }
                        if (target == pos) break;
                        long tv = heapVal[pos]; heapVal[pos] = heapVal[target]; heapVal[target] = tv;
                        int ti = heapIdx[pos]; heapIdx[pos] = heapIdx[target]; heapIdx[target] = ti;
                        pos = target;
                    }
                }
            }
        }

        int[] result = new int[k];
        for (int i = k - 1; i >= 0; i--) {
            result[i] = heapIdx[0];
            heapSize--;
            if (heapSize > 0) {
                heapIdx[0] = heapIdx[heapSize];
                heapVal[0] = heapVal[heapSize];
                int pos = 0;
                while (true) {
                    int left = 2 * pos + 1, right = left + 1, target = pos;
                    if (ascending) {
                        if (left < heapSize && heapVal[left] > heapVal[target]) target = left;
                        if (right < heapSize && heapVal[right] > heapVal[target]) target = right;
                    } else {
                        if (left < heapSize && heapVal[left] < heapVal[target]) target = left;
                        if (right < heapSize && heapVal[right] < heapVal[target]) target = right;
                    }
                    if (target == pos) break;
                    long tv = heapVal[pos]; heapVal[pos] = heapVal[target]; heapVal[target] = tv;
                    int ti = heapIdx[pos]; heapIdx[pos] = heapIdx[target]; heapIdx[target] = ti;
                    pos = target;
                }
            }
        }
        return result;
    }

    // ========================================================================
    // Window Function Support
    // ========================================================================

    /** Dispatch to type-specialized or generic sortRange based on order-by columns. */
    private static void sortRangeDispatch(int[] indices, int lo, int hi,
                                           Object[] orderKeys, boolean[] orderDirs) {
        if (orderKeys.length == 1 && orderKeys[0] instanceof long[]) {
            sortRangeSingleLong(indices, lo, hi, (long[]) orderKeys[0], orderDirs[0]);
        } else if (orderKeys.length == 1 && orderKeys[0] instanceof double[]) {
            sortRangeSingleDouble(indices, lo, hi, (double[]) orderKeys[0], orderDirs[0]);
        } else {
            sortRange(indices, lo, hi, orderKeys, orderDirs);
        }
    }

    public static int[] windowArgSort(long[] partKeys, Object[] orderKeys,
                                       boolean[] orderDirs, int length) {
        int[] indices = new int[length];
        for (int i = 0; i < length; i++) indices[i] = i;

        long maxPart = 0;
        for (int i = 0; i < length; i++) {
            if (partKeys[i] > maxPart) maxPart = partKeys[i];
        }

        if (maxPart < 1_000_000 && orderKeys.length <= 2) {
            int nParts = (int)(maxPart + 1);
            int[] counts = new int[nParts];
            for (int i = 0; i < length; i++) counts[(int)partKeys[i]]++;

            int[] offsets = new int[nParts + 1];
            for (int p = 0; p < nParts; p++) offsets[p + 1] = offsets[p] + counts[p];

            int[] partitioned = new int[length];
            int[] pos = new int[nParts];
            for (int i = 0; i < length; i++) {
                int p = (int)partKeys[i];
                partitioned[offsets[p] + pos[p]] = i;
                pos[p]++;
            }

            if (nParts >= 2 && length > ColumnOps.PARALLEL_THRESHOLD) {
                // Batch partitions into nThreads coarse tasks (not one task per partition)
                int nThreads = Math.min(POOL.getParallelism(), nParts);
                int partsPerThread = (nParts + nThreads - 1) / nThreads;
                @SuppressWarnings("unchecked")
                Future<?>[] futures = new Future[nThreads];
                for (int t = 0; t < nThreads; t++) {
                    final int pStart = t * partsPerThread;
                    final int pEnd = Math.min(pStart + partsPerThread, nParts);
                    futures[t] = POOL.submit(() -> {
                        for (int p = pStart; p < pEnd; p++) {
                            int lo = offsets[p], hi = offsets[p + 1];
                            if (hi - lo > 1) {
                                sortRangeDispatch(partitioned, lo, hi, orderKeys, orderDirs);
                            }
                        }
                    });
                }
                try {
                    for (Future<?> f : futures) if (f != null) f.get();
                } catch (Exception e) { throw new RuntimeException("Parallel sort failed", e); }
            } else {
                for (int p = 0; p < nParts; p++) {
                    int lo = offsets[p], hi = offsets[p + 1];
                    if (hi - lo > 1) {
                        sortRangeDispatch(partitioned, lo, hi, orderKeys, orderDirs);
                    }
                }
            }
            return partitioned;
        } else {
            Object[] combinedKeys = new Object[orderKeys.length + 1];
            boolean[] combinedDirs = new boolean[orderDirs.length + 1];
            combinedKeys[0] = partKeys;
            combinedDirs[0] = true;
            System.arraycopy(orderKeys, 0, combinedKeys, 1, orderKeys.length);
            System.arraycopy(orderDirs, 0, combinedDirs, 1, orderDirs.length);

            sortRange(indices, 0, length, combinedKeys, combinedDirs);
            return indices;
        }
    }

    /** Compare two values in a typed array at given indices. */
    private static int compareArrayValues(Object arr, int i, int j) {
        if (arr instanceof long[]) {
            return Long.compare(((long[])arr)[i], ((long[])arr)[j]);
        } else if (arr instanceof double[]) {
            return Double.compare(((double[])arr)[i], ((double[])arr)[j]);
        } else if (arr instanceof String[]) {
            String a = ((String[])arr)[i], b = ((String[])arr)[j];
            if (a == null) return b == null ? 0 : -1;
            if (b == null) return 1;
            return a.compareTo(b);
        }
        return 0;
    }

    private static void sortRange(int[] indices, int lo, int hi,
                                   Object[] orderKeys, boolean[] orderDirs) {
        int len = hi - lo;
        if (len <= 64) {
            for (int i = lo + 1; i < hi; i++) {
                int key = indices[i];
                int j = i - 1;
                while (j >= lo) {
                    int cmp = compareMulti(orderKeys, orderDirs, key, indices[j]);
                    if (cmp >= 0) break;
                    indices[j + 1] = indices[j];
                    j--;
                }
                indices[j + 1] = key;
            }
        } else {
            int[] aux = new int[len];
            System.arraycopy(indices, lo, aux, 0, len);
            mergeSort(aux, 0, len, indices, lo, orderKeys, orderDirs);
        }
    }

    private static void mergeSort(int[] src, int srcLo, int len,
                                   int[] dst, int dstLo,
                                   Object[] orderKeys, boolean[] orderDirs) {
        if (len <= 64) {
            System.arraycopy(src, srcLo, dst, dstLo, len);
            for (int i = dstLo + 1; i < dstLo + len; i++) {
                int key = dst[i];
                int j = i - 1;
                while (j >= dstLo) {
                    int cmp = compareMulti(orderKeys, orderDirs, key, dst[j]);
                    if (cmp >= 0) break;
                    dst[j + 1] = dst[j];
                    j--;
                }
                dst[j + 1] = key;
            }
            return;
        }
        int half = len >>> 1;
        mergeSort(dst, dstLo, half, src, srcLo, orderKeys, orderDirs);
        mergeSort(dst, dstLo + half, len - half, src, srcLo + half, orderKeys, orderDirs);
        merge(src, srcLo, srcLo + half, srcLo + len, dst, dstLo, orderKeys, orderDirs);
    }

    private static void merge(int[] src, int lo, int mid, int hi,
                               int[] dst, int dstLo,
                               Object[] orderKeys, boolean[] orderDirs) {
        int i = lo, j = mid, k = dstLo;
        while (i < mid && j < hi) {
            if (compareMulti(orderKeys, orderDirs, src[i], src[j]) <= 0) {
                dst[k++] = src[i++];
            } else {
                dst[k++] = src[j++];
            }
        }
        while (i < mid) dst[k++] = src[i++];
        while (j < hi)  dst[k++] = src[j++];
    }

    private static void sortRangeSingleLong(int[] indices, int lo, int hi,
                                             long[] vals, boolean asc) {
        int len = hi - lo;
        if (len <= 64) {
            for (int i = lo + 1; i < hi; i++) {
                int key = indices[i];
                long kv = vals[key];
                int j = i - 1;
                while (j >= lo) {
                    int cmp = Long.compare(kv, vals[indices[j]]);
                    if (!asc) cmp = -cmp;
                    if (cmp >= 0) break;
                    indices[j + 1] = indices[j];
                    j--;
                }
                indices[j + 1] = key;
            }
        } else {
            int[] aux = new int[len];
            System.arraycopy(indices, lo, aux, 0, len);
            mergeSortSingleLong(aux, 0, len, indices, lo, vals, asc);
        }
    }

    private static void mergeSortSingleLong(int[] src, int srcLo, int len,
                                              int[] dst, int dstLo,
                                              long[] vals, boolean asc) {
        if (len <= 64) {
            System.arraycopy(src, srcLo, dst, dstLo, len);
            for (int i = dstLo + 1; i < dstLo + len; i++) {
                int key = dst[i];
                long kv = vals[key];
                int j = i - 1;
                while (j >= dstLo) {
                    int cmp = Long.compare(kv, vals[dst[j]]);
                    if (!asc) cmp = -cmp;
                    if (cmp >= 0) break;
                    dst[j + 1] = dst[j];
                    j--;
                }
                dst[j + 1] = key;
            }
            return;
        }
        int half = len >>> 1;
        mergeSortSingleLong(dst, dstLo, half, src, srcLo, vals, asc);
        mergeSortSingleLong(dst, dstLo + half, len - half, src, srcLo + half, vals, asc);
        mergeSingleLong(src, srcLo, srcLo + half, srcLo + len, dst, dstLo, vals, asc);
    }

    private static void mergeSingleLong(int[] src, int lo, int mid, int hi,
                                          int[] dst, int dstLo,
                                          long[] vals, boolean asc) {
        int i = lo, j = mid, k = dstLo;
        while (i < mid && j < hi) {
            int cmp = Long.compare(vals[src[i]], vals[src[j]]);
            if (!asc) cmp = -cmp;
            if (cmp <= 0) {
                dst[k++] = src[i++];
            } else {
                dst[k++] = src[j++];
            }
        }
        while (i < mid) dst[k++] = src[i++];
        while (j < hi)  dst[k++] = src[j++];
    }

    private static void sortRangeSingleDouble(int[] indices, int lo, int hi,
                                               double[] vals, boolean asc) {
        int len = hi - lo;
        if (len <= 64) {
            for (int i = lo + 1; i < hi; i++) {
                int key = indices[i];
                double kv = vals[key];
                int j = i - 1;
                while (j >= lo) {
                    int cmp = Double.compare(kv, vals[indices[j]]);
                    if (!asc) cmp = -cmp;
                    if (cmp >= 0) break;
                    indices[j + 1] = indices[j];
                    j--;
                }
                indices[j + 1] = key;
            }
        } else {
            int[] aux = new int[len];
            System.arraycopy(indices, lo, aux, 0, len);
            mergeSortSingleDouble(aux, 0, len, indices, lo, vals, asc);
        }
    }

    private static void mergeSortSingleDouble(int[] src, int srcLo, int len,
                                                int[] dst, int dstLo,
                                                double[] vals, boolean asc) {
        if (len <= 64) {
            System.arraycopy(src, srcLo, dst, dstLo, len);
            for (int i = dstLo + 1; i < dstLo + len; i++) {
                int key = dst[i];
                double kv = vals[key];
                int j = i - 1;
                while (j >= dstLo) {
                    int cmp = Double.compare(kv, vals[dst[j]]);
                    if (!asc) cmp = -cmp;
                    if (cmp >= 0) break;
                    dst[j + 1] = dst[j];
                    j--;
                }
                dst[j + 1] = key;
            }
            return;
        }
        int half = len >>> 1;
        mergeSortSingleDouble(dst, dstLo, half, src, srcLo, vals, asc);
        mergeSortSingleDouble(dst, dstLo + half, len - half, src, srcLo + half, vals, asc);
        mergeSingleDouble(src, srcLo, srcLo + half, srcLo + len, dst, dstLo, vals, asc);
    }

    private static void mergeSingleDouble(int[] src, int lo, int mid, int hi,
                                            int[] dst, int dstLo,
                                            double[] vals, boolean asc) {
        int i = lo, j = mid, k = dstLo;
        while (i < mid && j < hi) {
            int cmp = Double.compare(vals[src[i]], vals[src[j]]);
            if (!asc) cmp = -cmp;
            if (cmp <= 0) {
                dst[k++] = src[i++];
            } else {
                dst[k++] = src[j++];
            }
        }
        while (i < mid) dst[k++] = src[i++];
        while (j < hi)  dst[k++] = src[j++];
    }

    /** Compare two row indices across multiple order-by columns. */
    private static int compareMulti(Object[] orderKeys, boolean[] orderDirs, int a, int b) {
        for (int k = 0; k < orderKeys.length; k++) {
            int cmp = compareArrayValues(orderKeys[k], a, b);
            if (!orderDirs[k]) cmp = -cmp;
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /**
     * Compute ROW_NUMBER window function over pre-sorted indices.
     */
    public static double[] windowRowNumber(long[] partKeys, int[] sortedIndices, int length) {
        if (length < ColumnOps.PARALLEL_THRESHOLD) return windowRowNumberSeq(partKeys, sortedIndices, length);
        // Find partition boundaries in sorted order
        int[] partStarts = findPartitionBoundaries(partKeys, sortedIndices, length);
        int nParts = partStarts.length - 1;
        // Only parallelize when partitions are large enough to amortize FJP overhead.
        // With many tiny partitions (avg < 8 rows), the per-element work is trivial
        // and parallel random writes to result[] cause cache contention.
        // Threshold 8 keeps WIN benchmarks (avg 4 rows) sequential while allowing
        // H2O-Q8 (avg 60 rows) to benefit from parallelism.
        long avgPartSize = length / Math.max(nParts, 1);
        if (avgPartSize < 8) return windowRowNumberSeq(partKeys, sortedIndices, length);
        double[] result = new double[length];
        int nThreads = Math.min(POOL.getParallelism(), Math.max(1, nParts));
        int partsPerThread = (nParts + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<?>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int ps = t * partsPerThread;
            final int pe = Math.min(ps + partsPerThread, nParts);
            futures[t] = POOL.submit(() -> {
                for (int p = ps; p < pe; p++) {
                    int lo = partStarts[p], hi = partStarts[p + 1];
                    for (int i = lo; i < hi; i++) {
                        result[sortedIndices[i]] = (double)(i - lo + 1);
                    }
                }
            });
        }
        try {
            for (Future<?> f : futures) if (f != null) f.get();
        } catch (Exception e) { throw new RuntimeException("Parallel windowRowNumber failed", e); }
        return result;
    }

    private static double[] windowRowNumberSeq(long[] partKeys, int[] sortedIndices, int length) {
        double[] result = new double[length];
        long prevPart = Long.MIN_VALUE;
        long rn = 0;
        for (int i = 0; i < length; i++) {
            int idx = sortedIndices[i];
            long p = partKeys[idx];
            rn = (p == prevPart) ? rn + 1 : 1;
            result[idx] = (double)rn;
            prevPart = p;
        }
        return result;
    }

    /** Find partition boundaries in sorted order. Returns array of start indices + length sentinel. */
    private static int[] findPartitionBoundaries(long[] partKeys, int[] sortedIndices, int length) {
        // Count partitions
        int nParts = 1;
        for (int i = 1; i < length; i++) {
            if (partKeys[sortedIndices[i]] != partKeys[sortedIndices[i - 1]]) nParts++;
        }
        int[] starts = new int[nParts + 1];
        int idx = 0;
        starts[0] = 0;
        for (int i = 1; i < length; i++) {
            if (partKeys[sortedIndices[i]] != partKeys[sortedIndices[i - 1]]) {
                starts[++idx] = i;
            }
        }
        starts[nParts] = length;
        return starts;
    }

    /**
     * Compute LAG/LEAD window function over pre-sorted indices.
     */
    public static double[] windowLagLead(long[] partKeys, int[] sortedIndices,
                                          Object values, int length,
                                          int offset, double defaultVal, boolean isLead) {
        if (length < ColumnOps.PARALLEL_THRESHOLD) return windowLagLeadSeq(partKeys, sortedIndices, values, length, offset, defaultVal, isLead);

        int[] partStarts = findPartitionBoundaries(partKeys, sortedIndices, length);
        int nParts = partStarts.length - 1;
        long avgPartSize = length / Math.max(nParts, 1);
        if (avgPartSize < 8) return windowLagLeadSeq(partKeys, sortedIndices, values, length, offset, defaultVal, isLead);

        double[] result = new double[length];
        boolean isLong = values instanceof long[];
        boolean isDouble = values instanceof double[];

        int nThreads = Math.min(POOL.getParallelism(), Math.max(1, nParts));
        int partsPerThread = (nParts + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<?>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int ps = t * partsPerThread;
            final int pe = Math.min(ps + partsPerThread, nParts);
            futures[t] = POOL.submit(() -> {
                for (int p = ps; p < pe; p++) {
                    int lo = partStarts[p], hi = partStarts[p + 1];
                    for (int i = lo; i < hi; i++) {
                        int refI = isLead ? i + offset : i - offset;
                        int curIdx = sortedIndices[i];
                        if (refI < lo || refI >= hi) {
                            result[curIdx] = defaultVal;
                        } else {
                            int refIdx = sortedIndices[refI];
                            if (isLong) result[curIdx] = (double)((long[])values)[refIdx];
                            else if (isDouble) result[curIdx] = ((double[])values)[refIdx];
                            else result[curIdx] = defaultVal;
                        }
                    }
                }
            });
        }
        try {
            for (Future<?> f : futures) if (f != null) f.get();
        } catch (Exception e) { throw new RuntimeException("Parallel windowLagLead failed", e); }
        return result;
    }

    private static double[] windowLagLeadSeq(long[] partKeys, int[] sortedIndices,
                                              Object values, int length,
                                              int offset, double defaultVal, boolean isLead) {
        double[] result = new double[length];
        int[] partStart = new int[length];
        int[] partEnd = new int[length];
        int pStart = 0;
        for (int i = 0; i < length; i++) {
            if (i == 0 || partKeys[sortedIndices[i]] != partKeys[sortedIndices[i-1]]) {
                if (i > 0) {
                    for (int j = pStart; j < i; j++) partEnd[j] = i;
                }
                pStart = i;
            }
            partStart[i] = pStart;
        }
        for (int j = pStart; j < length; j++) partEnd[j] = length;
        boolean isLong = values instanceof long[];
        boolean isDouble = values instanceof double[];
        for (int i = 0; i < length; i++) {
            int refI = isLead ? i + offset : i - offset;
            int curIdx = sortedIndices[i];
            if (refI < partStart[i] || refI >= partEnd[i]) {
                result[curIdx] = defaultVal;
            } else {
                int refIdx = sortedIndices[refI];
                if (isLong) result[curIdx] = (double)((long[])values)[refIdx];
                else if (isDouble) result[curIdx] = ((double[])values)[refIdx];
                else result[curIdx] = defaultVal;
            }
        }
        return result;
    }

    /**
     * Compute running SUM window function over pre-sorted indices.
     */
    public static double[] windowRunningSum(long[] partKeys, int[] sortedIndices,
                                             Object values, int length) {
        if (length < ColumnOps.PARALLEL_THRESHOLD) return windowRunningSumSeq(partKeys, sortedIndices, values, length);

        int[] partStarts = findPartitionBoundaries(partKeys, sortedIndices, length);
        int nParts = partStarts.length - 1;
        long avgPartSize = length / Math.max(nParts, 1);
        if (avgPartSize < 8) return windowRunningSumSeq(partKeys, sortedIndices, values, length);

        double[] result = new double[length];
        boolean isLong = values instanceof long[];
        boolean isDouble = values instanceof double[];

        int nThreads = Math.min(POOL.getParallelism(), Math.max(1, nParts));
        int partsPerThread = (nParts + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<?>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int ps = t * partsPerThread;
            final int pe = Math.min(ps + partsPerThread, nParts);
            futures[t] = POOL.submit(() -> {
                for (int p = ps; p < pe; p++) {
                    int lo = partStarts[p], hi = partStarts[p + 1];
                    double running = 0.0;
                    for (int i = lo; i < hi; i++) {
                        int idx = sortedIndices[i];
                        double v = isLong ? (double)((long[])values)[idx]
                                 : isDouble ? ((double[])values)[idx]
                                 : 0.0;
                        running += v;
                        result[idx] = running;
                    }
                }
            });
        }
        try {
            for (Future<?> f : futures) if (f != null) f.get();
        } catch (Exception e) { throw new RuntimeException("Parallel windowRunningSum failed", e); }
        return result;
    }

    private static double[] windowRunningSumSeq(long[] partKeys, int[] sortedIndices,
                                                 Object values, int length) {
        double[] result = new double[length];
        boolean isLong = values instanceof long[];
        boolean isDouble = values instanceof double[];
        long prevPart = Long.MIN_VALUE;
        double running = 0.0;
        for (int i = 0; i < length; i++) {
            int idx = sortedIndices[i];
            long p = partKeys[idx];
            double v = isLong ? (double)((long[])values)[idx]
                     : isDouble ? ((double[])values)[idx]
                     : 0.0;
            running = (p == prevPart) ? running + v : v;
            result[idx] = running;
            prevPart = p;
        }
        return result;
    }

    /**
     * Compute partition keys from multiple columns.
     */
    // Use Long.MIN_VALUE + 1 to avoid collision with NULL sentinel (Long.MIN_VALUE)
    private static final long HT_EMPTY = Long.MIN_VALUE + 1;

    /** Open-addressed hash table: assign sequential IDs to distinct long keys. */
    private static long[] assignSequentialIds(long[] keys, int length) {
        long[] result = new long[length];
        // Size hash table to ~70% load factor, power of 2
        int cap = Integer.highestOneBit(Math.max(64, (int) Math.min((long) length * 10 / 7, Integer.MAX_VALUE - 1)));
        if (cap < (long) length * 10 / 7) cap <<= 1;
        long[] htKeys = new long[cap];
        long[] htVals = new long[cap];
        Arrays.fill(htKeys, HT_EMPTY);
        int mask = cap - 1;
        int shift = Integer.numberOfLeadingZeros(cap) + 1;
        long nextId = 0;
        for (int i = 0; i < length; i++) {
            long key = keys[i];
            int slot = (int)((key * 0x9E3779B97F4A7C15L) >>> shift) & mask;
            while (true) {
                if (htKeys[slot] == HT_EMPTY) {
                    htKeys[slot] = key;
                    htVals[slot] = nextId;
                    result[i] = nextId++;
                    break;
                }
                if (htKeys[slot] == key) {
                    result[i] = htVals[slot];
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }
        return result;
    }

    public static long[] computePartitionKeys(Object[] partCols, int length) {
        long[] result = new long[length];
        if (partCols.length == 0) return result;

        if (partCols.length == 1) {
            Object col = partCols[0];
            if (col instanceof long[]) {
                // Open-addressed hash table — no autoboxing
                return assignSequentialIds((long[])col, length);
            } else if (col instanceof double[]) {
                // Convert to long bits, then use open-addressed HT
                double[] arr = (double[])col;
                long[] bits = new long[length];
                for (int i = 0; i < length; i++) bits[i] = Double.doubleToLongBits(arr[i]);
                return assignSequentialIds(bits, length);
            } else if (col instanceof String[]) {
                // Strings: keep HashMap (need String equality)
                String[] arr = (String[])col;
                java.util.HashMap<String, Long> smap = new java.util.HashMap<>();
                long nextId = 0;
                for (int i = 0; i < length; i++) {
                    String s = arr[i];
                    Long id = smap.get(s);
                    if (id == null) { id = nextId++; smap.put(s, id); }
                    result[i] = id;
                }
            }
        } else {
            // Multi-column: compute composite long key via hash, then assign IDs
            // For correctness with collisions, fall back to HashMap
            java.util.HashMap<java.util.List<Object>, Long> compositeMap = new java.util.HashMap<>();
            long nextId = 0;
            for (int i = 0; i < length; i++) {
                java.util.List<Object> key = new java.util.ArrayList<>(partCols.length);
                for (Object col : partCols) {
                    if (col instanceof long[]) {
                        key.add(((long[])col)[i]);
                    } else if (col instanceof double[]) {
                        key.add(((double[])col)[i]);
                    } else if (col instanceof String[]) {
                        key.add(((String[])col)[i]);
                    }
                }
                Long id = compositeMap.get(key);
                if (id == null) { id = nextId++; compositeMap.put(key, id); }
                result[i] = id;
            }
        }
        return result;
    }
}
