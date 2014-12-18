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

/**
 * The Atlas Plugin Manager. Responsible for initializing and terminating the {@link AtlasPlugin}.
 */
public enum AtlasPluginManager {
    /** The AtlasPluginManager instance. */
    INSTANCE;

    private final AtlasPlugin plugin;

    /**
     * Create a new manager for initializing and terminating the atlas plugin
     * using dynamic properties to drive its configuration.
     */
    AtlasPluginManager() {
        PluginConfig config = new DynamicPluginConfig();
        plugin = new AtlasPlugin(config);
    }

    /** Start the plugin. */
    public void start() {
        plugin.start();
    }

    /** Shutdown the plugin. */
    public void stop() {
        plugin.shutdown();
    }

    /** Get the atlas plugin. */
    public AtlasPlugin getPlugin() {
        return plugin;
    }
}
