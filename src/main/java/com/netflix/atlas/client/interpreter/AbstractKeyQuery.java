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

import com.google.common.base.Preconditions;
import com.netflix.servo.monitor.MonitorConfig;

/**
 * Base class for queries that perform a match against a tag key or name.
 */
abstract class AbstractKeyQuery implements Query {

    /**
     * The key.
     */
    private final String key;

    /**
     * Whether the key is is 'name'.
     */
    private final boolean isNameQuery;

    public AbstractKeyQuery(String key) {
        this.key = Preconditions.checkNotNull(key);
        isNameQuery = "name".equals(key);
    }

    protected String getValue(MonitorConfig config) {
        return isNameQuery ? config.getName() : config.getTags().getValue(key);
    }

    public String getKey() {
        return key;
    }

    public boolean isNameQuery() {
        return isNameQuery;
    }
}
