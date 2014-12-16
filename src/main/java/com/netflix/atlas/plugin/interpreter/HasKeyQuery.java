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
import com.netflix.servo.monitor.MonitorConfig;

import java.util.Map;

/**
 * A query that checks whether a given key is present in the metric taglist.
 */
final class HasKeyQuery extends AbstractKeyQuery {
    HasKeyQuery(String k) {
        super(k);
    }

    @Override
    public String toString() {
        return getKey() + ",:has";
    }

    @Override
    public boolean apply(Map<String, String> tags) {
        return tags.containsKey(getKey());
    }

    @Override
    public boolean apply(MonitorConfig config) {
        return isNameQuery() || config.getTags().containsKey(getKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof HasKeyQuery)) {
            return false;
        }
        HasKeyQuery query = (HasKeyQuery) obj;
        return getKey().equals(query.getKey());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getKey());
    }
}
