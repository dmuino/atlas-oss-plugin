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

import com.google.common.collect.Lists;
import com.netflix.servo.Metric;
import com.netflix.servo.publish.MetricObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A metric observer that dumps metrics to the local filesystem.
 */
final class FileMetricObserver implements MetricObserver {
    private static final String NAME = "FileMetricObserver";
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMetricObserver.class);
    private final AtomicReference<String> lastDir = new AtomicReference<>(null);
    private final AtomicReference<MetricObserver> fileMetricObserver =
            new AtomicReference<>(null);
    private final PluginConfig config;
    private final PushManager pushManager;

    /**
     * Create a file metric observer using the given config.
     */
    public FileMetricObserver(PluginConfig config, PushManager pushManager) {
        this.config = config;
        this.pushManager = pushManager;
        updateDirectory(config.getMetricsDir());
    }

    private void updateDirectory(String directoryName) {
        final File dir = new File(directoryName);
        if (!dir.isDirectory() && !dir.mkdirs()) {
            LOGGER.warn("Unable to create directory: {}", directoryName);
        } else {
            lastDir.set(directoryName);
            fileMetricObserver.set(
                    new com.netflix.servo.publish.FileMetricObserver("atlas", dir, true));
        }
    }

    @Override
    public void update(List<Metric> metrics) {
        final String currentDir = config.getMetricsDir();
        final String last = lastDir.get();
        if (!currentDir.equals(last)) {
            LOGGER.info("Updating directory location from {} to {}", last, currentDir);
            updateDirectory(currentDir);
        }
        final MetricObserver fileObserver = fileMetricObserver.get();
        if (fileObserver != null) {
            List<Metric> all;
            List<Metric> pushed = pushManager.getLatestPushedMetrics();
            if (pushed.isEmpty()) {
                all = metrics;
            } else {
                all = Lists.newArrayList(metrics);
                all.addAll(pushed);
            }
            fileObserver.update(all);
        } else {
            LOGGER.info("Unable to write metrics to local files. Directory not available: {}",
                    currentDir);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

}
