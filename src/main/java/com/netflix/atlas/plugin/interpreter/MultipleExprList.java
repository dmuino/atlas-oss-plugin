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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.servo.Metric;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A {@link ListValueExpression} wrapping one or more {@link ValueExpression}s.
 */
public final class MultipleExprList implements ListValueExpression {
    private static final List<String> KEYS = ImmutableList.of("label");
    private final List<Object> expressions;

    /**
     * Create a MultipleExprList from a Collection of ValueExpression.
     */
    MultipleExprList(Collection<Object> expressions) {
        this.expressions = ImmutableList.copyOf(expressions);
    }

    static MultipleExprList from(ValueExpression expression) {
        return new MultipleExprList(ImmutableList.<Object>of(expression));
    }

    static MultipleExprList fromCollection(Collection<Object> objects) {
        return new MultipleExprList(objects);
    }

    @Override
    public Map<List<String>, LabeledResult> apply(List<Metric> updates) {
        Map<List<String>, LabeledResult> resultMap = Maps.newHashMap();

        for (Object o : expressions) {
            if (o instanceof ValueExpression) {
                ValueExpression expression = (ValueExpression) o;
                final String label = expression.getLabel();
                resultMap.put(ImmutableList.of(label), new LabeledResult(label, expression.apply(updates)));
            } else {
                ListValueExpression expression = (ListValueExpression) o;
                Map<List<String>, LabeledResult> partial = expression.apply(updates);
                resultMap.putAll(partial);
            }

        }
        return resultMap;
    }

    @Override
    public ListValueExpression addFilter(Query query) {
        List<Object> newExpressions = Lists.newArrayListWithCapacity(expressions.size());
        for (Object o : expressions) {
            if (o instanceof ValueExpression) {
                ValueExpression expression = (ValueExpression) o;
                newExpressions.add(expression.addFilter(query));
            } else {
                ListValueExpression expression = (ListValueExpression) o;
                newExpressions.add(expression.addFilter(query));
            }
        }
        return new MultipleExprList(newExpressions);
    }

    @Override
    public List<String> getKeys() {
        return KEYS;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("expressions", expressions)
                .toString();
    }
}
