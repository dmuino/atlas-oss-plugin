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
import com.google.common.collect.ImmutableSet;
import com.netflix.servo.monitor.MonitorConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A query that checks whether a given key value is
 * one of many given values.
 */
final class InQuery extends AbstractKeyQuery {
    private final Set<String> values;

    InQuery(String k, Collection<String> values) {
        super(k);
        this.values = ImmutableSet.copyOf(values);
        Preconditions.checkArgument(values.size() >= 1);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getKey()).append(",(");
        for (String v : values) {
            builder.append(',').append(v);
        }
        builder.append(",),:in");
        return builder.toString();
    }

    @Override
    public boolean apply(Map<String, String> tags) {
        String value = tags.get(getKey());
        return (value != null) && values.contains(value);
    }

    @Override
    public boolean apply(MonitorConfig config) {
        final String v = getValue(config);
        return (v != null) && values.contains(v);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof InQuery)) {
            return false;
        }
        InQuery query = (InQuery) obj;
        return getKey().equals(query.getKey()) && values.equals(query.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getKey(), values);
    }
}
