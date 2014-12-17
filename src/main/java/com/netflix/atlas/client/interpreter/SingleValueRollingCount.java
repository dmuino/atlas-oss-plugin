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
 * Rolling count that applies to a valueExpression.
 */
final class SingleValueRollingCount implements ValueExpression {
    private final ValueExpression expression;
    private final RollingCount rollingCount;
    private final Context context;

    SingleValueRollingCount(Context context, ValueExpression expression, int period) {
        this.expression = Preconditions.checkNotNull(expression);
        this.context = Preconditions.checkNotNull(context);
        this.rollingCount = new RollingCount(context, expression.getLabel(), period);
    }

    @Override
    public double apply(List<Metric> updates) {
        rollingCount.update(Utils.isTrue(expression.apply(updates)));
        return rollingCount.getValue();
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return new SingleValueRollingCount(context, expression.addFilter(query),
                rollingCount.getPeriod());
    }

    @Override
    public String getLabel() {
        return rollingCount.getLabel();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SingleValueRollingCount that = (SingleValueRollingCount) o;
        return expression.equals(that.expression) && rollingCount.equals(that.rollingCount);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expression, rollingCount);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("rollingCount", rollingCount)
                .add("expression", expression)
                .toString();
    }
}
