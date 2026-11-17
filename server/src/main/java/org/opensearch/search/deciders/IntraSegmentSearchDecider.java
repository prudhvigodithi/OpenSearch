/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.deciders;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.internal.SearchContext;

/**
 * Evaluates whether queries and aggregations support intra-segment search.
 */
public class IntraSegmentSearchDecider {

    private boolean querySupport = true;
    private boolean aggSupport = true;
    private String reason = "no query or aggregation evaluated";
    private boolean hasQuery = false;
    private boolean hasAggregations = false;

    public void evaluateForQuery(QueryBuilder queryBuilder) {
        hasQuery = true;
        if (queryBuilder.supportsIntraSegmentSearch() == false) {
            querySupport = false;
            reason = queryBuilder.getName() + " does not support intra-segment search";
        }
    }

    public void evaluateForAggregations(AggregatorFactories aggregations) {
        evaluateForAggregations(aggregations, null);
    }

    /**
     * Evaluates the aggregation tree for the given request; {@code searchContext} lets top-level factories decline when a
     * whole-segment fast path would apply to the request (see {@link AggregatorFactories#allFactoriesSupportIntraSegmentSearch(SearchContext)}).
     */
    public void evaluateForAggregations(AggregatorFactories aggregations, SearchContext searchContext) {
        if (aggregations == null) {
            return;
        }
        hasAggregations = true;
        if (aggregations.allFactoriesSupportIntraSegmentSearch(searchContext) == false) {
            aggSupport = false;
            reason = "some aggregations do not support intra-segment search";
        }
    }

    public boolean shouldUseIntraSegmentSearch() {
        if (hasQuery && querySupport == false) {
            return false;
        }
        if (hasAggregations && aggSupport == false) {
            return false;
        }
        return hasQuery || hasAggregations;
    }

    public String getReason() {
        if (shouldUseIntraSegmentSearch()) {
            return "query/aggregations support intra-segment search";
        }
        return reason;
    }
}
