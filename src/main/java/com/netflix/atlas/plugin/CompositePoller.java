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

package com.netflix.atlas.plugin;

import com.google.common.collect.Lists;
import com.netflix.servo.Metric;
import com.netflix.servo.publish.MetricFilter;
import com.netflix.servo.publish.MetricPoller;

import java.util.List;

/**
 * A poller that aggregates metrics collected by a list of pollers.
 */
final class CompositePoller implements MetricPoller {
    private final List<MetricPoller> pollers;

    CompositePoller(List<MetricPoller> pollers) {
        this.pollers = pollers;
    }

    @Override
    public List<Metric> poll(MetricFilter filter) {
        return poll(filter, false);
    }

    @Override
    public List<Metric> poll(MetricFilter filter, boolean reset) {
        List<Metric> result = Lists.newArrayList();
        for (MetricPoller poller : pollers) {
            result.addAll(poller.poll(filter, reset));
        }

        return result;
    }
}
