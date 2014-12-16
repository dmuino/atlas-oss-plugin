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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.atlas.plugin.util.NetflixTagKey;
import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class PushManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushManager.class);
    private static final long POLLING_INTERVAL_MS = 60 * 1000L;
    private static final Tag RATE_TAG = DataSourceType.RATE;
    private static final List<Metric> EMPTY = ImmutableList.of();

    private final Counter pushedCount = Servo.getCounter("metrics.pushed");
    private final CounterCache cache = new CounterCache();
    private List<Metric> pushedMetrics = Lists.newArrayList();
    private List<Metric> lastPushed = pushedMetrics;

    private boolean isCounter(Metric m) {
        final TagList tags = m.getConfig().getTags();
        final String value = tags.getValue(DataSourceType.KEY);
        return value != null && "COUNTER".equals(value);
    }

    private MonitorConfig toRateConfig(MonitorConfig config) {
        return config.withAdditionalTag(RATE_TAG);
    }

    private List<Metric> countersToRate(List<Metric> metrics) {
        List<Metric> newMetrics = Lists.newArrayList();
        for (Metric m : metrics) {
            if (isCounter(m)) {
                final MonitorConfig rateConfig = toRateConfig(m.getConfig());
                final CounterValue prev = cache.get(rateConfig);
                if (prev != null) {
                    final double rate = prev.computeRate(m);
                    newMetrics.add(new Metric(rateConfig, m.getTimestamp(), rate));
                } else {
                    CounterValue current = new CounterValue(m);
                    cache.put(rateConfig, current);
                    final double delta = m.getNumberValue().doubleValue();
                    final double rate = current.computeRate(POLLING_INTERVAL_MS, delta);
                    newMetrics.add(new Metric(rateConfig, m.getTimestamp(), rate));
                }
            } else {
                newMetrics.add(m);
            }
        }
        return newMetrics;
    }

    synchronized List<Metric> getMetrics() {
        return pushedMetrics;
    }

    synchronized List<Metric> getMetricsAsRates() {
        if (pushedMetrics.isEmpty()) {
            return EMPTY;
        } else {
            List<Metric> metrics = countersToRate(pushedMetrics);
            LOGGER.debug("Got {} metrics from our push queue", metrics.size());
            return metrics;
        }
    }

    private List<Metric> stripCommonTags(List<Metric> metrics) {
        Map<String, String> commonTags = NetflixTagKey.tagsFromEnvironment();
        List<Metric> result = Lists.newArrayListWithCapacity(metrics.size());

        for (Metric metric : metrics) {
            TagList origTags = metric.getConfig().getTags();
            MonitorConfig.Builder builder = MonitorConfig.builder(metric.getConfig().getName());
            for (Tag tag : origTags) {
                if (!commonTags.containsKey(tag.getKey())) {
                    builder.withTag(tag);
                }
            }
            Metric noCommonTags = new Metric(builder.build(), metric.getTimestamp(), metric.getValue());
            result.add(noCommonTags);
        }
        return result;
    }

    synchronized void save(List<Metric> rawMetrics) {
        pushedCount.increment(rawMetrics.size());
        pushedMetrics.addAll(stripCommonTags(rawMetrics));
        LOGGER.debug("Got {} metrics pushed. Current size of push buffer: {}",
                rawMetrics.size(), pushedMetrics.size());
    }

    synchronized void reset() {
        lastPushed = pushedMetrics;
        pushedMetrics = Lists.newArrayList();
    }

    synchronized List<Metric> getLatestPushedMetrics() {
        LOGGER.debug("pushedMetrics = {} - lastPushed = {}", pushedMetrics.size(), lastPushed.size());
        return pushedMetrics.isEmpty() ? lastPushed : pushedMetrics;
    }

    synchronized List<Metric> getLatestPushedMetricsAsRates() {
        return countersToRate(getLatestPushedMetrics());
    }

    private static class CounterCache extends LinkedHashMap<MonitorConfig, CounterValue> {
        CounterCache() {
            super(16, 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<MonitorConfig, CounterValue> eldest) {
            final long now = System.currentTimeMillis();
            final long lastMod = eldest.getValue().getTimestamp();
            final boolean expired = (now - lastMod > (2 * POLLING_INTERVAL_MS));
            if (expired) {
                LOGGER.debug("heartbeat interval exceeded, expiring {}", eldest.getKey());
            }
            return expired;
        }
    }

    private static class CounterValue {
        private long timestamp;
        private double value;

        public CounterValue(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public CounterValue(Metric m) {
            this(m.getTimestamp(), m.getNumberValue().doubleValue());
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double computeRate(Metric m) {
            final long currentTimestamp = m.getTimestamp();
            final double currentValue = m.getNumberValue().doubleValue();

            final long durationMillis = currentTimestamp - timestamp;
            final double delta = currentValue - value;

            timestamp = currentTimestamp;
            value = currentValue;

            return computeRate(durationMillis, delta);
        }

        public double computeRate(long durationMillis, double delta) {
            final double millisPerSecond = 1000.0;
            final double duration = durationMillis / millisPerSecond;
            return (duration <= 0.0 || delta <= 0.0) ? 0.0 : delta / duration;
        }

    }
}
