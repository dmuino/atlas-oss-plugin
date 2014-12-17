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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.servo.Metric;
import com.netflix.servo.tag.Tag;

import java.util.List;
import java.util.Map;

/**
 * A group-by expression. It groups the results of applying the updates
 * to a common expression based on a set of keys.
 * It generates a Map of keys to labeled results.
 */
class GroupBy implements ListValueExpression {
    private static final Joiner JOINER = Joiner.on(',');
    private final List<String> keys;
    private final ValueExpression expression;
    private final Context context;

    public GroupBy(Context context, List<?> keys, ValueExpression expression) {
        this.expression = Preconditions.checkNotNull(expression);
        this.context = Preconditions.checkNotNull(context);

        Preconditions.checkNotNull(keys);
        Preconditions.checkArgument(!keys.isEmpty());

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Object k : keys) {
            builder.add((String) k);
        }
        this.keys = builder.build();
    }

    @Override
    public List<String> getKeys() {
        return keys;
    }

    private String getValue(Metric m, String key) {
        if (context.isCommonTag(key)) {
            return context.getCommonTagValue(key);
        } else if (key.equals("name")) {
            return m.getConfig().getName();
        } else {
            Tag tag = m.getConfig().getTags().getTag(key);
            if (tag != null) {
                return tag.getValue();
            } else {
                return null;
            }
        }
    }

    @Override
    public Map<List<String>, LabeledResult> apply(List<Metric> updates) {
        // group metrics by keys
        Map<List<String>, List<Metric>> grouped = Maps.newHashMap();
        for (Metric metric : updates) {
            boolean shouldKeep = true;
            List<String> groupByValues = Lists.newArrayListWithCapacity(keys.size());
            for (String key : keys) {
                String value = getValue(metric, key);
                if (value != null) {
                    groupByValues.add(value);
                } else {
                    // ignore this metric
                    shouldKeep = false;
                    break;
                }
            }

            if (shouldKeep) {
                List<Metric> valuesForKeys = grouped.get(groupByValues);
                if (valuesForKeys == null) {
                    valuesForKeys = Lists.newArrayList();
                    grouped.put(groupByValues, valuesForKeys);
                }
                valuesForKeys.add(metric);
            }
        }

        Map<List<String>, LabeledResult> results = Maps.newHashMapWithExpectedSize(grouped.size());
        for (Map.Entry<List<String>, List<Metric>> entry : grouped.entrySet()) {
            List<String> k = entry.getKey();
            List<Metric> v = entry.getValue();
            String label = String.format("GroupBy([%s], %s)",
                    JOINER.join(k), expression.getLabel());
            results.put(k, new LabeledResult(label, expression.apply(v)));
        }
        return results;
    }

    @Override
    public ListValueExpression addFilter(Query query) {
        return new GroupBy(context, keys, expression.addFilter(query));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupBy groupBy = (GroupBy) o;
        return expression.equals(groupBy.expression) && keys.equals(groupBy.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keys, expression);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("keys", keys).
                add("expression", expression).
                toString();
    }
}
