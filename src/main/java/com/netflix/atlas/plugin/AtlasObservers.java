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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.netflix.servo.Metric;
import com.netflix.servo.monitor.Pollers;
import com.netflix.servo.publish.AsyncMetricObserver;
import com.netflix.servo.publish.MetricObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

final class AtlasObservers implements MetricObserver {
    private static final int ASYNC_QUEUE_SIZE = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(AtlasObservers.class);

    private final BaseAtlasMetricObserver atlasMetricObserver;
    private final Collection<MetricObserver> observers;
    private final PushManager pushManager;

    AtlasObservers(PluginConfig pluginConfig, BaseAtlasMetricObserver atlasMetricObserver, PushManager pushManager) {
        this.atlasMetricObserver = atlasMetricObserver;
        this.pushManager = pushManager;
        this.observers = ImmutableList.of(
                createAtlas(pluginConfig, atlasMetricObserver),
                createAtlasCW(pluginConfig, pushManager),
                createFileMetrics(pluginConfig, pushManager));
    }

    private static MetricObserver createAtlas(final PluginConfig config,
                                              final BaseAtlasMetricObserver observer) {
        final Predicate atlasEnabled = new Predicate() {
            @Override
            public boolean apply() {
                return config.isEnabled();
            }
        };
        return condAsync("atlas", atlasEnabled, observer);
    }

    private static MetricObserver createAtlasCW(final PluginConfig config, final PushManager pushManager) {
        final Predicate atlasCloudWatchEnabled = new Predicate() {
            @Override
            public boolean apply() {
                return config.isCloudwatchEnabled() && !Strings.isNullOrEmpty(config.getCloudwatchExpr());
            }

        };

        return condAsync("atlasCloudWatch", atlasCloudWatchEnabled,
                new CloudWatchObserver(config, pushManager));
    }

    private static MetricObserver createFileMetrics(final PluginConfig config, final PushManager pushManager) {
        final Predicate fileObserverEnabled = new Predicate() {
            @Override
            public boolean apply() {
                return config.isFileMetricsEnabled();
            }
        };
        return condAsync("fileMetrics", fileObserverEnabled,
                new FileMetricObserver(config, pushManager));
    }

    /**
     * Create a new observer that will only receive updates if the
     * predicate is true. The updates will be processed asynchronously.
     */
    public static MetricObserver condAsync(String name, Predicate p,
                                           MetricObserver observer) {
        final long expire = Pollers.getPollingIntervals().get(0);
        final MetricObserver asyncObserver = new AsyncMetricObserver(
                name, observer, ASYNC_QUEUE_SIZE, expire);
        return new ConditionalObserver(p, asyncObserver);
    }

    @Override
    public void update(List<Metric> metrics) {
        if (metrics.isEmpty()) {
            LOGGER.info("metrics list is empty, no data being sent to server");
            return;
        }

        for (MetricObserver observer : observers) {
            LOGGER.debug("Forwarding {} metrics to {}", metrics.size(), observer.getName());
            observer.update(metrics);
        }

        pushManager.reset();
    }


    public String getName() {
        return "atlasObservers";
    }

    void push(List<Metric> metrics) {
        pushManager.save(metrics); // save pushed metrics so they can be used later
        atlasMetricObserver.push(metrics);
    }
}
