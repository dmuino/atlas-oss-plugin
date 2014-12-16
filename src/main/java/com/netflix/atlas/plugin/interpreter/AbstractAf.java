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

import com.google.common.base.Preconditions;
import com.netflix.servo.monitor.MonitorConfig;

/**
 * Base class for Aggregate functions.
 */
abstract class AbstractAf implements ValueExpression {
    private final Query filter;
    private final String label;

    AbstractAf(String function, Query filter) {
        this.filter = Preconditions.checkNotNull(filter);
        this.label = String.format("%s(%s)", Preconditions.checkNotNull(function), filter);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractAf that = (AbstractAf) o;
        return filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        return filter.hashCode();
    }

    @Override
    public String toString() {
        return label;
    }

    protected boolean matches(MonitorConfig config) {
        return Queries.matches(filter, config);
    }

    protected Query withFilter(Query query) {
        return Queries.and(filter, query);
    }
}
