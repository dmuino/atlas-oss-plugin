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
import com.netflix.servo.monitor.MonitorConfig;

import java.util.Map;

final class NotQuery implements Query {
    private final Query q;

    NotQuery(Query q) {
        this.q = Preconditions.checkNotNull(q);
    }

    @Override
    public String toString() {
        return q + ",:not";
    }

    @Override
    public boolean apply(Map<String, String> tags) {
        return !q.apply(tags);
    }

    @Override
    public boolean apply(MonitorConfig config) {
        return !q.apply(config);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof NotQuery)) {
            return false;
        }
        NotQuery query = (NotQuery) obj;
        return q.equals(query.q);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(q);
    }
}
