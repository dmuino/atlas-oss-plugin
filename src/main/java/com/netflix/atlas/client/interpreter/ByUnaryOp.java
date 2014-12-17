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
import com.google.common.collect.Maps;
import com.netflix.servo.Metric;

import java.util.List;
import java.util.Map;

/**
 * Unary operation on a single scalar value.
 */
final class ByUnaryOp implements ListValueExpression {
    private final ListValueExpression a;
    private final UnaryOp op;
    ByUnaryOp(ListValueExpression a, UnaryOp op) {
        this.a = Preconditions.checkNotNull(a);
        this.op = Preconditions.checkNotNull(op);
    }

    static ByUnaryOp create(ListValueExpression a, UnaryOp op) {
        return new ByUnaryOp(a, op);
    }

    @Override
    public Map<List<String>, LabeledResult> apply(List<Metric> updates) {
        Map<List<String>, LabeledResult> aResults = a.apply(updates);
        Map<List<String>, LabeledResult> results = Maps.newHashMapWithExpectedSize(aResults.size());
        for (Map.Entry<List<String>, LabeledResult> entry : aResults.entrySet()) {
            LabeledResult aRes = entry.getValue();
            String resLabel = String.format(op.getFormat(), aRes.getLabel());
            results.put(entry.getKey(),
                    new LabeledResult(resLabel, op.apply(aRes.getValue())));
        }

        return results;
    }

    @Override
    public ListValueExpression addFilter(Query query) {
        return create(a.addFilter(query), op);
    }

    @Override
    public List<String> getKeys() {
        return a.getKeys();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ByUnaryOp byUnaryOp = (ByUnaryOp) o;
        return a.equals(byUnaryOp.a) && op == byUnaryOp.op;
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
