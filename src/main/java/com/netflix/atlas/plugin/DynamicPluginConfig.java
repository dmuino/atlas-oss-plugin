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

import com.google.common.base.Preconditions;
import com.netflix.atlas.plugin.interpreter.Queries;
import com.netflix.atlas.plugin.interpreter.Query;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@code PluginConfig} driven by {@link com.netflix.config.DynamicProperty} properties.
 */
public class DynamicPluginConfig implements PluginConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicPluginConfig.class);
    private static final DynamicPropertyFactory PROP_FACTORY = DynamicPropertyFactory.getInstance();

    private static final String PREFIX = "netflix.atlas.plugin.";
    private static final DynamicBooleanProperty ENABLED = PROP_FACTORY.getBooleanProperty(
            PREFIX + "enabled", true);
    private static final DynamicBooleanProperty FILE_METRICS_ENABLED =
            PROP_FACTORY.getBooleanProperty(PREFIX + "fileMetricsEnabled", false);
    private static final DynamicStringProperty METRICS_DIR =
            PROP_FACTORY.getStringProperty(PREFIX + "metricsDir", "/data/atlas");
    private static final DynamicStringProperty CW_EXPR =
            PROP_FACTORY.getStringProperty(PREFIX + "cloudwatchExpr", ":false,:sum");
    private static final DynamicStringProperty CW_NAMESPACE =
            PROP_FACTORY.getStringProperty(PREFIX + "cloudwatchNamespace", "ATLAS");

    private static final QueryProperty FILTER_EXPR =
            new QueryProperty(PREFIX + "filterExpr", "level,DEBUG,:eq,:not");
    private static final DynamicIntProperty PUSH_QUEUE_SIZE =
            PROP_FACTORY.getIntProperty(PREFIX + "pushQueueSize", 1000);
    private static final DynamicIntProperty BATCH_SIZE =
            PROP_FACTORY.getIntProperty(PREFIX + "batchSize", 10000);
    private static final RollupConfigProperty ROLLUP_CONFIG =
            new RollupConfigProperty(PREFIX + "rollupConfig", null);
    private static final DynamicStringProperty PUBLISH_URI =
            PROP_FACTORY.getStringProperty(PREFIX + "fastPublishUri",
                    "http://atlas.example.org/api/v1/publish-fast");

    @Override
    public boolean isEnabled() {
        return ENABLED.get();
    }

    @Override
    public boolean isFileMetricsEnabled() {
        return FILE_METRICS_ENABLED.get();
    }

    @Override
    public String getMetricsDir() {
        return METRICS_DIR.get();
    }

    @Override
    public boolean isCloudwatchEnabled() {
        return !CW_EXPR.get().isEmpty();
    }

    @Override
    public int getPushQueueSize() {
        return PUSH_QUEUE_SIZE.get();
    }

    @Override
    public String getPublishUri() {
        return PUBLISH_URI.get();
    }

    @Override
    public int getBatchSize() {
        return BATCH_SIZE.get();
    }

    @Override
    public Callable<Query> getFilterExpr() {
        return FILTER_EXPR;
    }

    @Override
    public String getCloudwatchExpr() {
        return CW_EXPR.get();
    }

    @Override
    public String getCloudwatchNamespace() {
        return CW_NAMESPACE.get();
    }

    @Override
    public RollupConfig getRollupConfig() {
        return ROLLUP_CONFIG.get();
    }

    @Override
    public boolean isDropByDefault() {
        return false;
    }

    private static class QueryProperty implements Callable<Query> {
        private final String name;
        private final String dflt;
        private final DynamicStringProperty prop;
        private final AtomicReference<Query> query = new AtomicReference<>();

        public QueryProperty(String name, String dflt) {
            this.name = Preconditions.checkNotNull(name);
            this.dflt = Preconditions.checkNotNull(dflt);
            prop = PROP_FACTORY.getStringProperty(name, dflt);
            prop.addCallback(new Runnable() {
                @Override
                public void run() {
                    refresh();
                }
            });
            refresh();
        }

        @Override
        public Query call() {
            return query.get();
        }

        private void refresh() {
            final String expr = prop.get();
            try {
                query.set(Queries.parse(expr));
            } catch (Exception e) {
                query.set(Queries.parse(dflt));
                LOGGER.warn("failed to parse query [" + expr + "] set for property "
                        + name + ", using default value [" + dflt + "]", e);
            }
        }
    }

    private static class RollupConfigProperty {
        private final AtomicReference<RollupConfig> config = new AtomicReference<>();
        private final DynamicStringProperty prop;
        private final String name;
        private Runnable callback;

        RollupConfigProperty(String name, String dflt) {
            this.name = name;
            this.prop = PROP_FACTORY.getStringProperty(name, dflt);
            prop.addCallback(new Runnable() {
                @Override
                public void run() {
                    refresh();
                }
            });
            callback = null;
            refresh();
        }

        RollupConfig get() {
            return config.get();
        }

        private void refresh() {
            final String expr = prop.get();
            try {
                if (expr != null) {
                    RollupConfig newConfig = RollupConfig.create(expr);
                    config.set(newConfig);
                } else {
                    config.set(null);
                }
                if (callback != null) {
                    callback.run();
                }
            } catch (Exception e) {
                LOGGER.warn("failed to parse rollupConfig [" + expr + "]: error ["
                        + e.getMessage() + "] for property "
                        + name + ", using previous value [" + config.get() + "]");
            }
        }

        void setCallback(Runnable r) {
            callback = r;
        }
    }
}
