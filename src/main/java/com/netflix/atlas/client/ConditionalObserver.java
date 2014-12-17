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

package com.netflix.atlas.client;

import com.netflix.servo.Metric;
import com.netflix.servo.publish.MetricObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An observer that will only send metrics to a downstream observer if a given condition
 * is true.
 */
public class ConditionalObserver implements MetricObserver {
    private static final String NAME = "ConditionalObserver";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionalObserver.class);
    private final Predicate condition;
    private final MetricObserver observer;

    /**
     * Create a conditional observer using the given condition and downstream observer.
     */
    public ConditionalObserver(Predicate condition, MetricObserver observer) {
        this.condition = condition;
        this.observer = observer;
    }

    @Override
    public void update(List<Metric> metrics) {
        if (condition.apply()) {
            LOGGER.debug("Forwarding {} metrics to {}", metrics.size(), observer.getName());
            observer.update(metrics);
        } else {
            LOGGER.debug("Predicate for {} returned false. Not forwarding metrics.",
                    observer.getName());
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
