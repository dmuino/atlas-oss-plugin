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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.netflix.servo.Metric;

import java.util.List;

/**
 * A binary operation that operates on two scalar values (ValueExpressions).
 */
final class ValueValueRelOp implements ValueExpression {
    private final ValueExpression a;
    private final ValueExpression b;
    private final BinOp op;
    private final String label;

    private ValueValueRelOp(ValueExpression a, ValueExpression b, BinOp op) {
        this.a = Preconditions.checkNotNull(a);
        this.b = Preconditions.checkNotNull(b);
        this.op = Preconditions.checkNotNull(op);
        this.label = String.format("%s %s %s", a.getLabel(), op.getSymbol(), b.getLabel());
    }

    static ValueExpression create(ValueExpression a, ValueExpression b, BinOp op) {
        // constant folding
        if (a instanceof ConstantExpression && b instanceof ConstantExpression) {
            ConstantExpression ca = (ConstantExpression) a;
            ConstantExpression cb = (ConstantExpression) b;
            return ConstantExpression.from(op.apply(ca.getValue(), cb.getValue()));
        } else if (a.equals(ConstantExpression.NAN) || b.equals(ConstantExpression.NAN)) {
            if (op != BinOp.ADD) {
                // NaN <op> Number -> NaN (except :add which treats NaN as 0)
                return ConstantExpression.NAN;
            } else {
                return a.equals(ConstantExpression.NAN) ? b : a;
            }
        } else {
            return new ValueValueRelOp(a, b, op);
        }
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return create(a.addFilter(query), b.addFilter(query), op);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public double apply(List<Metric> updates) {
        double aVal = a.apply(updates);
        double bVal = b.apply(updates);
        return op.apply(aVal, bVal);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ValueValueRelOp that = (ValueValueRelOp) o;
        return op == that.op && a.equals(that.a) && b.equals(that.b);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(a, b, op);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("a", a).
                add("b", b).
                add("op", op).
                toString();
    }
}
