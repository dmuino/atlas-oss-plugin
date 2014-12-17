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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.netflix.atlas.plugin.util.NetflixTagKey;
import com.netflix.atlas.plugin.util.ValidCharacters;
import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.BasicGauge;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Gauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Pollers;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.tag.BasicTag;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Observer that forwards metrics to atlas. In addition to being MetricObserver, it also supports
 * a push model that sends metrics as soon as possible (asynchronously).
 */
abstract class BaseAtlasMetricObserver implements MetricObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseAtlasMetricObserver.class);
    private static final Tag ATLAS_COUNTER_TAG = new BasicTag("atlas.dstype", "counter");
    private static final Tag ATLAS_GAUGE_TAG = new BasicTag("atlas.dstype", "gauge");
    private static final UpdateTasks NO_TASKS = new UpdateTasks(0, null, -1L);
    private static final int HTTP_OK = 200;
    private static final int PERCENTAGE = 100;
    private static final int MAX_PERC_TO_SEND = 90;
    private final PluginConfig config;
    private final RollupPolicy rollupPolicy;
    private final long sendTimeoutMs; // in milliseconds
    private final long stepMs; // in milliseconds
    private final Counter numMetricsTotal = Monitors.newCounter("numMetricsTotal");
    private final Timer updateTimer = Monitors.newTimer("update");
    private final Counter pushSizeTotal = Monitors.newCounter("pushSize");
    private final Counter pushSizeCount = Monitors.newCounter("pushCount");
    private final Counter numMetricsDroppedSendTimeout = newCounter("numMetricsDropped",
            "sendTimeout");
    private final Counter numMetricsDroppedQueueFull = newCounter("numMetricsDropped",
            "sendQueueFull");
    private final Counter numMetricsDroppedHttpErr = newCounter("numMetricsDropped",
            "httpError");
    private final Counter numMetricsSent = Monitors.newCounter("numMetricsSent");
    private final TagList commonTags;
    private final BlockingQueue<UpdateTasks> pushQueue;
    @SuppressWarnings("UnusedDeclaration")
    private final Gauge<Integer> pushQueueSize = new BasicGauge<>(
            MonitorConfig.builder("pushQueue").build(), new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
            return pushQueue.size();
        }
    });

    BaseAtlasMetricObserver(PluginConfig config, RollupConfigurator rollupConfigurator, int pollerIdx) {
        this.config = config;
        this.stepMs = Pollers.getPollingIntervals().get(pollerIdx);
        this.sendTimeoutMs = stepMs * MAX_PERC_TO_SEND / PERCENTAGE;
        commonTags = createCommonTagListFromEnvironment();
        pushQueue = new LinkedBlockingQueue<>(config.getPushQueueSize());
        rollupPolicy = new RollupPolicy(rollupConfigurator);
        final Thread pushThread = new Thread(new PushProcessor(), "BaseAtlasMetricObserver-Push");
        pushThread.setDaemon(true);
        pushThread.start();
    }

    protected static Counter newCounter(String name, String err) {
        return new BasicCounter(MonitorConfig.builder(name).withTag("error", err).build());
    }

    private static Metric asGauge(Metric m) {
        return new Metric(m.getConfig().withAdditionalTag(ATLAS_GAUGE_TAG),
                m.getTimestamp(), m.getValue());
    }

    private static Metric asCounter(Metric m) {
        return new Metric(m.getConfig().withAdditionalTag(ATLAS_COUNTER_TAG),
                m.getTimestamp(), m.getValue());
    }

    private static boolean isCounter(Metric m) {
        final TagList tags = m.getConfig().getTags();
        final String value = tags.getValue(DataSourceType.KEY);
        return value != null && value.equals(DataSourceType.COUNTER.name());
    }

    private static boolean isGauge(Metric m) {
        final TagList tags = m.getConfig().getTags();
        final String value = tags.getValue(DataSourceType.KEY);
        return value != null && value.equals(DataSourceType.GAUGE.name());
    }

    private static boolean isRate(Metric m) {
        final TagList tags = m.getConfig().getTags();
        final String value = tags.getValue(DataSourceType.KEY);
        return value != null && value.equals(DataSourceType.RATE.name());
    }

    private static List<Metric> identifyDsTypes(List<Metric> metrics) {
        List<Metric> result = Lists.newArrayListWithCapacity(metrics.size());
        for (Metric m : metrics) {
            // since we never generate atlas.dstype = counter we can do the following:
            result.add(isRate(m) ? m : asGauge(m));
        }
        return result;
    }

    /**
     * Get the number of milliseconds for the step size.
     */
    protected long getStepMs() {
        return stepMs;
    }

    /**
     * Get the plugin config.
     */
    protected PluginConfig getPluginConfig() {
        return config;
    }

    private TagList createCommonTagListFromEnvironment() {
        return BasicTagList.copyOf(NetflixTagKey.tagsFromEnvironment());
    }

    @Override
    public String getName() {
        return "atlas";
    }

    private List<Metric> identifyCountersForPush(List<Metric> metrics) {
        List<Metric> transformed = Lists.newArrayListWithCapacity(metrics.size());
        for (Metric m : metrics) {
            Metric toAdd = m;
            if (isCounter(m)) {
                toAdd = asCounter(m);
            } else if (isGauge(m)) {
                toAdd = asGauge(m);
            }
            transformed.add(toAdd);
        }
        return transformed;
    }

    /**
     * Immediately send metrics to the backend.
     *
     * @param rawMetrics Metrics to be sent. The names are sanitized,
     *                   and rollup policies
     *                   applied before sending them to the backend publish cluster.
     */
    public void push(List<Metric> rawMetrics) {
        List<Metric> metricsWithValidValues = ValidCharacters.toValidValues(filter(rawMetrics));
        List<Metric> rolledUp = rollupPolicy.rollup(metricsWithValidValues);
        LOGGER.debug("Scheduling push of {} metrics", rolledUp.size());
        final UpdateTasks tasks = getUpdateTasks(BasicTagList.EMPTY,
                identifyCountersForPush(rolledUp));
        final int maxAttempts = 5;
        int attempts = 0;
        while (!pushQueue.offer(tasks) && ++attempts < maxAttempts) {
            final UpdateTasks droppedTasks = pushQueue.remove();
            LOGGER.warn("Removing old push task due to queue full. Dropping {} metrics.",
                    droppedTasks.numMetrics);
            numMetricsDroppedQueueFull.increment(droppedTasks.numMetrics);
        }
        if (attempts >= maxAttempts) {
            LOGGER.error("Unable to push update of {}", tasks);
            numMetricsDroppedQueueFull.increment(tasks.numMetrics);
        } else {
            LOGGER.debug("Queued push of {}", tasks);
        }
    }

    private void sendNow(UpdateTasks updateTasks) {
        if (updateTasks.numMetrics == 0) {
            return;
        }
        pushSizeCount.increment();
        pushSizeTotal.increment(updateTasks.numMetrics);

        final Stopwatch s = updateTimer.start();
        int totalSent = 0;
        try {
            totalSent = RxHttp.sendAll(updateTasks.tasks, updateTasks.numMetrics, sendTimeoutMs);
            LOGGER.debug("Sent {}/{} metrics to atlas", totalSent, updateTasks.numMetrics);
        } finally {
            s.stop();
            int dropped = updateTasks.numMetrics - totalSent;
            numMetricsDroppedSendTimeout.increment(dropped);
        }
    }

    protected boolean shouldIncludeMetric(Metric metric) {
        return metric.hasNumberValue();
    }

    /**
     * Return metrics to be sent to the main atlas deployment.
     * Metrics will be sent if their publishing policy matches atlas and if they
     * will *not* be sent to the aggregation cluster.
     */
    protected List<Metric> filter(List<Metric> metrics) {
        final List<Metric> filtered = Lists.newArrayListWithExpectedSize(metrics.size());
        for (Metric metric : metrics) {
            if (shouldIncludeMetric(metric)) {
                filtered.add(metric);
            }
        }
        LOGGER.debug("Filter: input {} metrics, output {} metrics",
                metrics.size(), filtered.size());
        return filtered;
    }

    @Override
    public void update(List<Metric> metrics) {
        List<Metric> rolledUp = rollupPolicy.rollup(identifyDsTypes(filter(metrics)));
        sendNow(getUpdateTasks(commonTags, rolledUp));
    }

    private UpdateTasks getUpdateTasks(TagList tags, List<Metric> metrics) {
        if (metrics.isEmpty()) {
            LOGGER.debug("metrics list is empty, no data being sent to server");
            return NO_TASKS;
        }

        final int numMetrics = metrics.size();
        final Metric[] atlasMetrics = new Metric[metrics.size()];
        metrics.toArray(atlasMetrics);

        numMetricsTotal.increment(numMetrics);
        final List<rx.Observable<Integer>> tasks = Lists.newArrayList();
        final String uri = config.getPublishUri();
        LOGGER.debug("writing {} metrics to atlas ({})", numMetrics, uri);
        int i = 0;
        while (i < numMetrics) {
            final int remaining = numMetrics - i;
            final int batchSize = Math.min(remaining, config.getBatchSize());
            final Metric[] batch = new Metric[batchSize];
            System.arraycopy(atlasMetrics, i, batch, 0, batchSize);
            final rx.Observable<Integer> sender = getSenderObservable(tags, batch);
            tasks.add(sender);
            i += batchSize;
        }
        assert (i == numMetrics);
        LOGGER.debug("succeeded in creating {} observable(s) to send metrics with total size {}",
                tasks.size(), numMetrics);

        return new UpdateTasks(numMetrics * getNumberOfCopies(), tasks, System.currentTimeMillis());
    }

    protected int getNumberOfCopies() {
        return 1;
    }

    abstract rx.Observable<Integer> getSenderObservable(TagList tags, Metric[] batch);

    /**
     * Utility function to map an Observable&lt;ByteBuf> to an Observable&lt;Integer> while also
     * updating our counters for metrics sent and errors.
     */
    protected Func1<HttpClientResponse<ByteBuf>, Integer> withBookkeeping(final int batchSize) {
        return new Func1<HttpClientResponse<ByteBuf>, Integer>() {
            @Override
            public Integer call(HttpClientResponse<ByteBuf> response) {
                boolean ok = response.getStatus().code() == HTTP_OK;
                if (ok) {
                    numMetricsSent.increment(batchSize);
                } else {
                    LOGGER.info("Status code: {} - Lost {} metrics",
                            response.getStatus().code(), batchSize);
                    numMetricsDroppedHttpErr.increment(batchSize);
                }

                return batchSize;
            }
        };
    }

    private static class UpdateTasks {
        private final int numMetrics;
        private final List<Observable<Integer>> tasks;
        private final long timestamp;

        UpdateTasks(int numMetrics, List<Observable<Integer>> tasks, long timestamp) {
            this.numMetrics = numMetrics;
            this.tasks = tasks;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            final int tasksSize = (tasks != null) ? tasks.size() : 0;
            return Objects.toStringHelper(this).
                    add("numMetrics", numMetrics).
                    add("tasks.size", tasksSize).
                    add("timestamp", timestamp).
                    toString();
        }
    }

    private class PushProcessor implements Runnable {
        @Override
        public void run() {
            boolean interrupted = false;
            while (!interrupted) {
                try {
                    sendNow(pushQueue.take());
                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted trying to get next UpdateTask to push");
                    interrupted = true;
                } catch (Throwable t) {
                    LOGGER.info("Caught unexpected exception pushing metrics", t);
                }
            }
        }
    }

}
