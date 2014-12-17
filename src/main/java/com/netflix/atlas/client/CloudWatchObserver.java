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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.netflix.atlas.client.interpreter.Context;
import com.netflix.atlas.client.interpreter.Interpreter;
import com.netflix.atlas.client.interpreter.LabeledResult;
import com.netflix.atlas.client.interpreter.ListValueExpression;
import com.netflix.atlas.client.util.NetflixEnvironment;
import com.netflix.atlas.client.util.NetflixTagKey;
import com.netflix.atlas.client.util.Strings;
import com.netflix.servo.Metric;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.publish.cloudwatch.CloudWatchMetricObserver;
import com.netflix.servo.tag.BasicTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forward native atlas metrics to cloudwatch.
 */
public final class CloudWatchObserver implements MetricObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudWatchObserver.class);

    private final PluginConfig config;
    private final CloudWatchMetricObserver cloudWatchMetricObserver;
    private final String asgFromEnv = NetflixTagKey.tagsFromEnvironment().get(NetflixTagKey.ASG);
    private final String asg = Strings.isNotEmpty(asgFromEnv) ? asgFromEnv : "unknown";

    private final BasicTagList tags = BasicTagList.of("AutoScalingGroupName", asg);
    private final AtomicInteger lastSent = Servo.getNumberGauge("atlas.cloudwatch.sent", new AtomicInteger(0));
    private final PushManager pushManager;

    /**
     * Create the observer with a given configuration.
     */
    public CloudWatchObserver(PluginConfig config, PushManager pushManager) {
        this.config = config;
        this.pushManager = pushManager;
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final AmazonCloudWatchClient cloudWatchClient =
                new AmazonCloudWatchClient(credentialsProvider);
        final String region = NetflixEnvironment.region();
        if (region != null) {
            cloudWatchClient.setEndpoint(String.format("monitoring.%s.amazonaws.com", region));
        }
        this.cloudWatchMetricObserver = new CloudWatchMetricObserver(getName(),
                config.getCloudwatchNamespace(), cloudWatchClient);
    }


    Metric toCloudwatchMetric(LabeledResult result, long timestamp) {
        return new Metric(result.getLabel(), tags, timestamp, result.getValue());
    }

    List<Metric> getCloudwatchMetrics(List<Metric> metrics, long now) {
        final Context context = Context.newDefaultContext();

        final List<Object> program = Interpreter.getTokens(config.getCloudwatchExpr());
        final ListValueExpression expression = context.getListExpression(program);
        final Map<List<String>, LabeledResult> data = expression.apply(metrics);
        final List<Metric> cwMetrics = Lists.newArrayList();
        for (LabeledResult entry : data.values()) {
            if (!Double.isNaN(entry.getValue())) {
                cwMetrics.add(toCloudwatchMetric(entry, now));
            }
        }
        return cwMetrics;
    }

    @Override
    public void update(List<Metric> servoMetrics) {
        List<Metric> pushedMetrics = pushManager.getMetricsAsRates();
        List<Metric> allMetrics;
        if (pushedMetrics.isEmpty()) {
            allMetrics = servoMetrics;
        } else {
            allMetrics = Lists.newArrayList(servoMetrics);
            allMetrics.addAll(pushedMetrics);
        }

        List<Metric> cwMetrics = getCloudwatchMetrics(allMetrics, System.currentTimeMillis());
        if (!cwMetrics.isEmpty()) {
            String cwSent = "";
            if (LOGGER.isInfoEnabled()) {
                cwSent = Joiner.on(", ").join(cwMetrics);
            }
            cloudWatchMetricObserver.update(cwMetrics);
            LOGGER.info("Sent to cloudwatch: {}", cwSent);
        } else {
            LOGGER.debug("No cloudwatch metrics found");
        }
        lastSent.set(cwMetrics.size());
    }

    @Override
    public String getName() {
        return "cloudwatch";
    }
}
