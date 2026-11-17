/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;

/**
 * Implemented by top-level aggregators whose collected (pre-build) state can be combined across the slices of a
 * concurrent segment search request.
 * <p>
 * Under intra-segment search a single segment is split into doc-id-range partitions collected by different slices,
 * each with its own aggregator instance. Normally every slice builds its own result (applying cutoffs such as the terms
 * aggregation's {@code shard_size}) and the per-slice results are reduced afterwards, which makes bucket results
 * approximate for high-cardinality fields. When {@link #canMergeIntraSegmentSlices()} returns true, the
 * {@link BucketCollectorProcessor} instead skips the per-slice build, merges the collected state of all slice instances
 * of the aggregator into one of them via {@link #mergeIntraSegmentSlice(Aggregator)} and builds a single result from the
 * merged state, so the shard result is identical to non-concurrent execution.
 * <p>
 * Implementations must return the same answer from {@link #canMergeIntraSegmentSlices()} for every slice instance of a
 * request, and all instances must share the same bucket key space (for example global ordinals of the same reader).
 *
 * @opensearch.internal
 */
@InternalApi
public interface IntraSegmentSliceMergeable {

    /**
     * Whether this aggregator defers building its result until the collected state of all slices has been merged.
     */
    boolean canMergeIntraSegmentSlices();

    /**
     * Adds the collected state of {@code other}, the same aggregator collected by another slice of the same request, into
     * this aggregator. Called after post-collection of all slices and before {@link Aggregator#buildTopLevel()}. The
     * {@code other} aggregator is not built afterwards.
     */
    void mergeIntraSegmentSlice(Aggregator other) throws IOException;
}
