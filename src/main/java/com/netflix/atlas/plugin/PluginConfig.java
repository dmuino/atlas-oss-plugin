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

import com.netflix.atlas.plugin.interpreter.Query;

import java.util.concurrent.Callable;

/**
 * Defines the configuration options that are available for the plugin.
 */
public interface PluginConfig extends RollupConfigurator {
    /**
     * Returns true if the plugin is enabled, i.e., it should be collecting
     * metrics each minute.
     */
    boolean isEnabled();

    /**
     * Whether to write metrics to local files.
     */
    boolean isFileMetricsEnabled();

    /**
     * Directory where to store metric dumps.
     */
    String getMetricsDir();

    /**
     * Whether to forward atlas metrics to CloudWatch.
     */
    boolean isCloudwatchEnabled();

    /**
     * Push queue size.
     */
    int getPushQueueSize();

    /**
     * The URI to use for publishing metrics to Atlas.
     */
    String getPublishUri();

    /**
     * Number of metrics to send in one batch.
     */
    int getBatchSize();

    /**
     * Filter expression to restrict the set of metrics that are published.
     */
    Callable<Query> getFilterExpr();

    /**
     * Atlas Expression used to compute cloudwatch metrics. For example: <br/><br/>
     * {@code
     * name, logQueueSize,:eq,:sum,logQueueSize,:legend
     * }
     * <br/><br/>
     * will send a single logQueueSize metric to cloudwatch using the sum of all metrics
     * that have that name.
     */
    String getCloudwatchExpr();


    /**
     * Namespace to use when publishing metrics to cloudwatch.
     */
    String getCloudwatchNamespace();
}
