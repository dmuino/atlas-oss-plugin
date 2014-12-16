/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.atlas.plugin.interpreter;

import com.netflix.servo.Metric;

import java.util.List;
import java.util.Map;

/**
 * Common interface for expressions that generate a list of results.
 */
public interface ListValueExpression extends Expression {
    /**
     * Evaluate the expression with the metrics provided.
     *
     * @param updates List of metrics to apply to our expression.
     * @return A Map of (list of actual values for the group-by keys) to LabeledResults.
     */
    Map<List<String>, LabeledResult> apply(List<Metric> updates);

    /**
     * Adds a common query to all subqueries of this expression.
     *
     * @param query The query to and to each subquery.
     * @return A new ListValueExpression with the updated queries.
     */
    ListValueExpression addFilter(Query query);

    /**
     * Get the list of keys for the group-by expression.
     */
    List<String> getKeys();
}
