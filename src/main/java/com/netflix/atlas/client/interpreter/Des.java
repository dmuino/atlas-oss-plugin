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

package com.netflix.atlas.client.interpreter;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.netflix.servo.Metric;

import java.util.List;

/**
 * Double exponential smoothing. Not implemented yet.
 */
final class Des implements ValueExpression {
    private final ValueExpression expression;
    private final int trainingSize;
    private final double alpha;
    private final double beta;
    private final String label;

    Des(ValueExpression expression, int trainingSize, double alpha, double beta) {
        this.expression = Preconditions.checkNotNull(expression);
        Preconditions.checkArgument(trainingSize > 0);

        this.trainingSize = trainingSize;
        this.alpha = alpha;
        this.beta = beta;
        this.label = String.format("DES(%s, %d, %.1f, %.1f)",
                expression.getLabel(), trainingSize, alpha, beta);
    }

    @Override
    public double apply(List<Metric> updates) {
        // FIXME
        return Double.NaN;
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return new Des(expression.addFilter(query), trainingSize, alpha, beta);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Des that = (Des) o;

        return expression.equals(that.expression)
                && trainingSize == that.trainingSize
                && Double.compare(alpha, that.alpha) == 0
                && Double.compare(beta, that.beta) == 0;

    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expression, trainingSize, alpha, beta);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("expression", expression).
                add("trainingSize", trainingSize).
                add("alpha", alpha).
                add("beta", beta).
                toString();
    }
}
