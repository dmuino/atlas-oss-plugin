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

/**
 * An expression that evaluates to a single scalar value.
 */
public interface ValueExpression extends Expression {
    /**
     * Apply the expression to a group of metrics returning a scalar result.
     */
    double apply(List<Metric> updates);

    /**
     * Get a label for the expression.
     */
    String getLabel();

    /**
     * Apply an additional filter to this expression.
     */
    ValueExpression addFilter(Query query);
}
