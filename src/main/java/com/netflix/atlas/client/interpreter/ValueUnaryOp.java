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
 * Unary operation on a single scalar value.
 */
final class ValueUnaryOp implements ValueExpression {
    private final ValueExpression a;
    private final UnaryOp op;
    ValueUnaryOp(ValueExpression a, UnaryOp op) {
        this.a = Preconditions.checkNotNull(a);
        this.op = Preconditions.checkNotNull(op);
    }

    static ValueExpression create(ValueExpression a, UnaryOp op) {
        if (a instanceof ConstantExpression) {
            ConstantExpression ce = (ConstantExpression) a;
            return ConstantExpression.from(op.apply(ce.getValue()));
        }

        return new ValueUnaryOp(a, op);
    }

    @Override
    public double apply(List<Metric> updates) {
        return op.apply(a.apply(updates));
    }

    @Override
    public String getLabel() {
        return String.format(op.getFormat(), a.getLabel());
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return create(a.addFilter(query), op);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ValueUnaryOp that = (ValueUnaryOp) o;
        return a.equals(that.a) && op == that.op;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(a, op);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("a", a)
                .add("op", op)
                .toString();
    }
}
