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
import com.netflix.servo.Metric;

import java.util.List;

/**
 * A constant expression.
 */
public final class ConstantExpression implements ValueExpression {
    /**
     * Not a number.
     */
    public static final ConstantExpression NAN = new ConstantExpression(Double.NaN);
    /**
     * A constant expression that represents true.
     */
    public static final ConstantExpression TRUE = new ConstantExpression(Utils.toBooleanVal(true));
    /**
     * A constant expression that represents true.
     */
    public static final ConstantExpression FALSE =
            new ConstantExpression(Utils.toBooleanVal(false));
    private final double val;
    private final String label;

    private ConstantExpression(double val) {
        this.val = val;
        this.label = String.format("%.1f", val);
    }

    /**
     * Return a constant expression representing the truth value of the given argument.
     */
    public static ConstantExpression fromBoolean(boolean b) {
        return b ? TRUE : FALSE;
    }

    /**
     * Create a constant expression from a double.
     */
    public static ConstantExpression from(double val) {
        if (Double.isNaN(val)) {
            return NAN;
        } else {
            return new ConstantExpression(val);
        }
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return this;
    }

    @Override
    public String getLabel() {
        return label;
    }

    public double getValue() {
        return val;
    }

    @Override
    public double apply(List<Metric> updates) {
        return val;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConstantExpression that = (ConstantExpression) o;
        return Double.compare(that.val, val) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(val);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("val", val).
                toString();
    }
}
