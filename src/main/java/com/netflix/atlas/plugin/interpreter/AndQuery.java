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

final class AndQuery implements Query {
    private final Query q1;
    private final Query q2;

    AndQuery(Query q1, Query q2) {
        this.q1 = Preconditions.checkNotNull(q1);
        this.q2 = Preconditions.checkNotNull(q2);
    }

    @Override
    public String toString() {
        return q1 + "," + q2 + ",:and";
    }

    @Override
    public boolean apply(Map<String, String> tags) {
        return q1.apply(tags) && q2.apply(tags);
    }

    @Override
    public boolean apply(MonitorConfig config) {
        return q1.apply(config) && q2.apply(config);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof AndQuery)) {
            return false;
        }
        AndQuery query = (AndQuery) obj;
        return q1.equals(query.q1) && q2.equals(query.q2);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(q1, q2);
    }
}
