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
import com.netflix.servo.monitor.MonitorConfig;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * A query that determines whether a key value matches a
 * given regular expression.
 */
final class RegexQuery extends AbstractKeyQuery {
    private final String v;
    private final Pattern p;

    RegexQuery(String k, String v) {
        super(k);
        this.v = Preconditions.checkNotNull(v);
        this.p = Pattern.compile(v);
    }

    @Override
    public String toString() {
        return getKey() + "," + v + ",:re";
    }

    @Override
    public boolean apply(Map<String, String> tags) {
        final String value = tags.get(getKey());
        return (value != null) && p.matcher(value).find();
    }

    @Override
    public boolean apply(MonitorConfig config) {
        final String s = getValue(config);
        return (s != null) && p.matcher(s).find();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof RegexQuery)) {
            return false;
        }
        RegexQuery query = (RegexQuery) obj;
        return getKey().equals(query.getKey()) && v.equals(query.v);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getKey(), v);
    }
}
