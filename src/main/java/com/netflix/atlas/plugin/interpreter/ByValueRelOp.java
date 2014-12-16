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
import com.google.common.collect.Maps;
import com.netflix.servo.Metric;

import java.util.List;
import java.util.Map;

/**
 * A binary operation that operates on a group-by expression and a scalar value.
 * When applied to an update, it returns
 * a map of keys to labeled results.
 */
final class ByValueRelOp implements ListValueExpression {
    private final ListValueExpression a;
    private final ValueExpression b;
    private final BinOp op;

    private ByValueRelOp(ListValueExpression a, ValueExpression b, BinOp op) {
        this.a = Preconditions.checkNotNull(a);
        this.b = Preconditions.checkNotNull(b);
        this.op = Preconditions.checkNotNull(op);
    }

    static ByValueRelOp create(ListValueExpression a, ValueExpression b, BinOp op) {
        return new ByValueRelOp(a, b, op);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ByValueRelOp that = (ByValueRelOp) o;
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

    @Override
    public Map<List<String>, LabeledResult> apply(List<Metric> updates) {
        Map<List<String>, LabeledResult> aResults = a.apply(updates);
        double bRes = b.apply(updates);

        Map<List<String>, LabeledResult> results = Maps.newHashMapWithExpectedSize(aResults.size());
        for (Map.Entry<List<String>, LabeledResult> entry : aResults.entrySet()) {
            LabeledResult aRes = entry.getValue();
            String resLabel = String.format("%s %s %s",
                    aRes.getLabel(), op.getSymbol(), b.getLabel());
            results.put(entry.getKey(),
                    new LabeledResult(resLabel, op.apply(aRes.getValue(), bRes)));
        }
        return results;
    }

    @Override
    public ListValueExpression addFilter(Query query) {
        return create(a.addFilter(query), b.addFilter(query), op);
    }

    @Override
    public List<String> getKeys() {
        return a.getKeys();
    }
}
