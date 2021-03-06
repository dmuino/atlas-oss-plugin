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

import com.netflix.servo.monitor.MonitorConfig;

import java.util.Map;

/**
 * A query expression for a {@link MonitorConfig}.
 */
public interface Query extends Expression {

    /**
     * Returns true if the tag list matches the query expression.
     */
    boolean apply(MonitorConfig config);


    /**
     * Returns true if the tags match the query expression.
     */
    boolean apply(Map<String, String> tags);
}
