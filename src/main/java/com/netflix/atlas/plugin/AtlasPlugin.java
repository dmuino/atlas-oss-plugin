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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.Metric;
import com.netflix.servo.MonitorRegistry;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Pollers;
import com.netflix.servo.publish.CounterToRateMetricTransform;
import com.netflix.servo.publish.JvmMetricPoller;
import com.netflix.servo.publish.MetricFilter;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.publish.MetricPoller;
import com.netflix.servo.publish.MonitorRegistryMetricPoller;
import com.netflix.servo.publish.NormalizationTransform;
import com.netflix.servo.publish.PollRunnable;
import com.netflix.servo.util.ClockWithOffset;
import com.netflix.servo.util.ThreadCpuStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The Atlas Plugin. Gather registered metrics and forward them to the Atlas backends.
 */
public class AtlasPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(AtlasPlugin.class);
    private static final long MIN_HEARTBEAT = 30;
    private static final long TIME_TO_SEND_MS = 2000L;
    private static final int DELAY_PERC = 90;
    private static final int PERCENT = 100;

    /**
     * ExecutorService used for critical metrics and main poller. Extra pollers added by the user
     * will also use this.
     */
    private final ScheduledExecutorService executor;

    private final PluginConfig config;
    private final MetricObserver observer;
    private final AtlasObservers atlasObserver;
    private final MetricFilter filter;

    private MetricPoller thePoller = null;

    /**
     * Create the plugin with a given config.
     */
    public AtlasPlugin(final PluginConfig config) {
        this.config = config;
        atlasObserver = new AtlasObservers(config, new AtlasMetricObserver(config),
                new PushManager());
        observer = getObserver();
        filter = new QueryMetricFilter(config.getFilterExpr());

        // We use a single thread for the plugin poller loop to keep resource usage low, writes
        // to external collectors are wrapped with async observer
        executor = getScheduledExecutorService("AtlasPluginPoller-%d", 1);
    }

    private static ScheduledExecutorService getScheduledExecutorService(String nameFormat, int nThreads) {
        final ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(nameFormat)
                .build();
        return Executors.newScheduledThreadPool(nThreads, factory);
    }

    private MetricObserver getObserver() {
        long interval = TimeUnit.MILLISECONDS.toSeconds(Pollers.getPollingIntervals().get(0));
        long heartbeat = interval * 2;
        if (heartbeat < MIN_HEARTBEAT) {
            heartbeat = MIN_HEARTBEAT;
        }

        MetricObserver o = new NormalizationTransform(atlasObserver, interval, heartbeat, TimeUnit.SECONDS);
        return new CounterToRateMetricTransform(o, heartbeat, TimeUnit.SECONDS);
    }

    private void addPoller(MetricPoller poller, long delay, TimeUnit unit) {
        PollRunnable task = new PollRunnable(poller, filter, true, ImmutableList.of(observer));
        executor.scheduleWithFixedDelay(task, 0L, delay, unit);
    }

    /**
     * Get 90% of the time expressed in ms, and convert it to seconds.
     */
    private static long delaySecondsFor(long ms) {
        return TimeUnit.MILLISECONDS.toSeconds(ms * DELAY_PERC / PERCENT);
    }

    /**
     * Start the plugin.
     */
    public void start() {
        if (!config.isEnabled()) {
            LOGGER.info("Atlas Plugin is disabled.");
            return;
        }

        final List<MetricPoller> pollers = Lists.newArrayList();
        final MonitorRegistry registry = DefaultMonitorRegistry.getInstance();
        final MetricPoller poller = new MonitorRegistryMetricPoller(registry);
        pollers.add(poller);

        final MetricPoller jvmPoller = new JvmMetricPoller();
        pollers.add(jvmPoller);

        thePoller = new CompositePoller(pollers);
        final long delayForMainPoller = delaySecondsFor(Pollers.getPollingIntervals().get(0));
        addPoller(thePoller, delayForMainPoller, TimeUnit.SECONDS);
        registry.register(Monitors.newObjectMonitor(observer));

        LOGGER.info("Starting to keep track of the cpu usage for threads in the jvm.");
        ThreadCpuStats.getInstance().start();
        LOGGER.info("Atlas Plugin started.");
    }

    private void flushMetrics() {
        try {
            List<Metric> metrics = thePoller.poll(filter, true);
            LOGGER.info("Scheduling {} metrics to be sent by our main observer.", metrics.size());
            observer.update(metrics);
            Thread.sleep(TIME_TO_SEND_MS);
            LOGGER.info("Sent ", metrics.size());
        } catch (Throwable t) {
            LOGGER.warn("failed to send metrics to our main observer", t);
        }
    }

    /**
     * Push metrics to Atlas asynchronously. This does not wait for the polling interval,
     * and does not add any node specific tags. The metrics are sent without
     * converting into rates.
     *
     * @param metrics List of metrics to send.
     */
    public void pushMetrics(List<Metric> metrics) {
        atlasObserver.push(metrics); // send immediately to the atlas backend
    }

    /**
     * Shutdown the plugin by terminating tasks.
     */
    public void shutdown() {
        LOGGER.info("Shutting down atlas-client.");
        DynamicCounter.increment("atlas.client.shutdown");

        executor.shutdownNow();

        LOGGER.info("Flushing pending metrics during shutdown");
        flushMetrics();

        List<Long> pollingIntervals = Pollers.getPollingIntervals();
        LOGGER.info("Flushing next set of metrics for main poller.");
        ClockWithOffset.INSTANCE.setOffset(pollingIntervals.get(0)); // main poller
        flushMetrics();
    }
}
