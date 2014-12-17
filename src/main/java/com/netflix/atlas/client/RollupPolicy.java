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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.atlas.client.interpreter.Query;
import com.netflix.servo.Metric;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.tag.SmallTagMap;
import com.netflix.servo.tag.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A rollup policy.
 */
public class RollupPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RollupPolicy.class);
    private final RollupConfigurator configurator;

    private final Counter metricsRaw = Monitors.newCounter("metricsRaw");
    private final Counter metricsProcessed = Monitors.newCounter("metricsProcessed");

    /**
     * Creates a new instance of a rollup policy with a given configurator.
     * @param configurator A {@link RollupConfigurator} used to get
     *                     the configuration for this policy.
     */
    public RollupPolicy(RollupConfigurator configurator) {
        this.configurator = configurator;
    }

    private static double sum(List<Metric> values) {
        double res = 0.0;
        for (Metric m : values) {
            res += m.getNumberValue().doubleValue(); // NaN?
        }
        return res;
    }

    private static double avg(List<Metric> values) {
        return sum(values) / values.size(); // rewrite if we do ops aware of NaN
    }

    private static double count(List<Metric> values) {
        return values.size();
    }

    private static double min(List<Metric> values) {
        double res = Double.MAX_VALUE;
        for (Metric m : values) {
            res = Math.min(m.getNumberValue().doubleValue(), res);
        }
        return Double.compare(res, Double.MAX_VALUE) == 0 ? Double.NaN : res;
    }

    private static double max(List<Metric> values) {
        double res = Double.MIN_VALUE;
        for (Metric m : values) {
            res = Math.max(m.getNumberValue().doubleValue(), res);
        }
        return Double.compare(res, Double.MIN_VALUE) == 0 ? Double.NaN : res;
    }

    private MonitorConfig keepTags(MonitorConfig monitorConfig, Set<String> tags) {
        boolean droppedSomeTags = false;

        SmallTagMap.Builder newTags = SmallTagMap.builder();
        for (Tag tag : monitorConfig.getTags()) {
            String tagName = tag.getKey();
            if (tags.contains(tagName)) {
                newTags.add(tag);
            } else {
                droppedSomeTags = true;
            }
        }

        if (droppedSomeTags) {
            return MonitorConfig.builder(monitorConfig.getName())
                    .withTags(newTags)
                    .withPublishingPolicy(monitorConfig.getPublishingPolicy())
                    .build();
        } else {
            // avoid creating extra objects if we don't have to
            return monitorConfig;
        }
    }

    private MonitorConfig dropTags(MonitorConfig monitorConfig, Set<String> tagsToDrop) {
        SmallTagMap.Builder newTags = SmallTagMap.builder();
        boolean droppedSomeTags = false;
        for (Tag tag : monitorConfig.getTags()) {
            String tagName = tag.getKey();
            if (!tagsToDrop.contains(tagName)) {
                newTags.add(tag);
            } else {
                droppedSomeTags = true;
            }
        }

        if (droppedSomeTags) {
            return MonitorConfig.builder(monitorConfig.getName())
                    .withTags(newTags)
                    .withPublishingPolicy(monitorConfig.getPublishingPolicy())
                    .build();
        } else {
            // avoid creating extra objects if we don't have to
            return monitorConfig;
        }
    }

    private Metric aggregate(RollupConfig.Aggr aggr, List<Metric> values) {
        assert (!values.isEmpty());
        if (aggr == RollupConfig.Aggr.DROP) {
            return null;
        }

        double res;
        switch (aggr) {
            case SUM:
                res = sum(values);
                break;
            case AVG:
                res = avg(values);
                break;
            case COUNT:
                res = count(values);
                break;
            case MAX:
                res = max(values);
                break;
            case MIN:
                res = min(values);
                break;
            default:
                throw new IllegalArgumentException("Unknown aggregate: " + aggr);
        }

        final Metric sample = values.get(0);
        return new Metric(sample.getConfig(), sample.getTimestamp(), res);
    }

    /**
     * Apply this policy to a given {@link java.util.List} of metrics.
     * @param metrics {@link java.util.List} of metrics.
     * @return The resulting metrics after applying this policy.
     */
    public List<Metric> rollup(List<Metric> metrics) {
        RollupConfig rollupConfig = configurator.getRollupConfig();

        metricsRaw.increment(metrics.size());
        if (rollupConfig == null) {
            // no policy, therefore raw == processed
            LOGGER.debug("No rollup policy - no change for {} metrics", metrics.size());
            metricsProcessed.increment(metrics.size());
            return metrics;
        } else {
            LOGGER.debug("Applying policy {} to {} metrics", rollupConfig, metrics.size());
        }

        List<RollupConfig.Rule> rules = rollupConfig.getRules();
        @SuppressWarnings("unchecked") List<Metric>[] metricsRules = new List[rules.size() + 1];
        // metricRules[rules.size()] will contain unmatched metrics
        for (int i = 0; i <= rules.size(); ++i) {
            metricsRules[i] = Lists.newArrayList();
        }

        for (Metric metric : metrics) {
            // we only look at metrics with number values
            if (!metric.hasNumberValue()) {
                continue;
            }

            boolean matched = false;
            for (int i = 0; i < rules.size(); i++) {
                RollupConfig.Rule rule = rules.get(i);
                Set<String> tags = ImmutableSet.copyOf(rule.getTags());
                MonitorConfig metricConfig = metric.getConfig();
                if (rule.getQuery().apply(metricConfig)) {
                    MonitorConfig newConfig = rule.isKeep()
                            ? keepTags(metricConfig, tags)
                            : dropTags(metricConfig, tags);
                    // only create a new metric if we got a different config
                    Metric newMetric = newConfig == metricConfig ? metric
                            : new Metric(newConfig, metric.getTimestamp(), metric.getValue());
                    metricsRules[i].add(newMetric);
                    matched = true;
                    break;
                }
            }
            if (!matched && !configurator.isDropByDefault()) {
                metricsRules[rules.size()].add(metric);
            }
        }

        // initialize with the unmatched metrics
        List<Metric> reduced = Lists.newArrayList(metricsRules[rules.size()]);

        // process the list of metrics matched by each rule
        for (int ruleIdx = 0; ruleIdx < rules.size(); ruleIdx++) {
            List<Metric> mapped = metricsRules[ruleIdx];
            Map<MonitorConfig, List<Metric>> configMetrics = Maps.newHashMap();
            for (Metric m : mapped) {
                List<Metric> listMetrics = configMetrics.get(m.getConfig());
                if (listMetrics == null) {
                    listMetrics = Lists.newArrayList();
                    configMetrics.put(m.getConfig(), listMetrics);
                }
                listMetrics.add(m);
            }


            // reduce
            for (Map.Entry<MonitorConfig, List<Metric>> entry : configMetrics.entrySet()) {
                Metric aggregated = aggregate(rules.get(ruleIdx).getAggr(), entry.getValue());
                if (aggregated != null) {
                    reduced.add(aggregated);
                }
            }
        }

        metricsProcessed.increment(reduced.size());
        LOGGER.debug("Got {} input metrics - reduced to {} metrics",
                metrics.size(), reduced.size());
        return reduced;
    }

    Query getFilter() {
        return configurator.getRollupConfig().getFilter();
    }
}
