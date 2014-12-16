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

package com.netflix.atlas.plugin.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * Utility class to deal with Netflix specific tags.
 */
public final class NetflixTagKey {

    /**
     * Netflix App tag key.
     */
    public static final String APP = "nf.app";

    /**
     * Netflix AMI tag key.
     */
    public static final String AMI = "nf.ami";

    /**
     * Autoscaling group tag key.
     */
    public static final String ASG = "nf.asg";

    /**
     * Cluster tag key.
     */
    public static final String CLUSTER = "nf.cluster";

    /**
     * Country tag key.
     */
    public static final String COUNTRY = "nf.country";

    /**
     * Node/Instance Id tag key.
     */
    public static final String NODE = "nf.node";

    /**
     * Region tag key.
     */
    public static final String REGION = "nf.region";

    /**
     * EC2 instance type tag key.
     */
    public static final String VM_TYPE = "nf.vmtype";

    /**
     * Availability zone tag key.
     */
    public static final String ZONE = "nf.zone";

    /**
     * Set that includes all the standard Netflix tags.
     */
    public static final Set<String> STANDARD_TAGS =
            ImmutableSet.of(APP, AMI, ASG, CLUSTER, COUNTRY, NODE, REGION, VM_TYPE, ZONE);

    private NetflixTagKey() {
    }

    private static void put(ImmutableMap.Builder<String, String> builder, String k, String v) {
        if (v != null && v.length() > 0) {
            builder.put(k, v);
        }
    }

    /**
     * Returns a map with the standard tags based on the environment variables
     * that are typically present on a netflix instance.
     */
    public static Map<String, String> tagsFromEnvironment() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        put(builder, APP, NetflixEnvironment.app());
        put(builder, AMI, NetflixEnvironment.ami());
        put(builder, ASG, NetflixEnvironment.asg());
        put(builder, CLUSTER, NetflixEnvironment.cluster());
        put(builder, NODE, NetflixEnvironment.instanceId());
        put(builder, REGION, NetflixEnvironment.region());
        put(builder, VM_TYPE, NetflixEnvironment.vmtype());
        put(builder, ZONE, NetflixEnvironment.zone());
        return builder.build();
    }
}
