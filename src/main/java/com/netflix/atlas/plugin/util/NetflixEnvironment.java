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

import com.google.common.base.Throwables;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class to deal with environment variables
 * available when running on a cloud instance.
 */
public final class NetflixEnvironment {

    public static final String DC_ZONE = "us-nflx-1a";
    private static final String OWNER = "EC2_OWNER_ID";
    private static final String AMI = "EC2_AMI_ID";
    private static final String VM_TYPE = "EC2_INSTANCE_TYPE";
    private static final String REGION = "EC2_REGION";
    private static final String ZONE = "EC2_AVAILABILITY_ZONE";
    private static final String INSTANCE_ID = "EC2_INSTANCE_ID";
    private static final String APP = "NETFLIX_APP";
    private static final String CLUSTER = "NETFLIX_CLUSTER";
    private static final String ASG = "NETFLIX_AUTO_SCALE_GROUP";
    private static final String STACK = "NETFLIX_STACK";
    private static final String ENV = "NETFLIX_ENVIRONMENT";
    private static final String UNKNOWN = null;

    private NetflixEnvironment() {
    }

    private static String getenv(String k, String dflt) {
        String v = System.getenv(k);
        return (v == null) ? dflt : v;
    }

    private static String firstNotNull(String... vs) {
        for (String v : vs) {
            if (v != null) return v;
        }
        return null;
    }

    /**
     * Returns the account id from the AWS metadata service or if that is not available
     * the EC2_OWNER_ID environment variable. If neither are available the netflix datacenter
     * will be returned.
     */
    public static String accountId() {
        return firstNotNull(AwsMetadata.accountId(), System.getenv(OWNER), "dc");
    }

    /**
     * Get the current AMI.
     *
     * @return Current AMI or null if none can be found.
     */
    public static String ami() {
        return firstNotNull(AwsMetadata.ami(), System.getenv(AMI));
    }

    /**
     * Get the VM Instance Type.
     *
     * @return The VM instance type of null if none can be found.
     */
    public static String vmtype() {
        return firstNotNull(AwsMetadata.vmtype(), System.getenv(VM_TYPE));
    }

    /**
     * Get the region.
     *
     * @return The region where the VM is running on us-nflx-1 if
     * we cannot find the env. var.
     */
    public static String region() {
        return firstNotNull(AwsMetadata.region(), System.getenv(REGION), "us-nflx-1");
    }

    /**
     * Get the zone.
     *
     * @return The availability zone or us-nflx-1a if we cannot determine the AZ.
     */
    public static String zone() {
        return firstNotNull(AwsMetadata.zone(), System.getenv(ZONE), DC_ZONE);
    }

    /**
     * Get the instance Id.
     *
     * @return The instance Id or the local hostname if we cannot determine the instanceId.
     */
    public static String instanceId() {
        String v = firstNotNull(AwsMetadata.instanceId(), System.getenv(INSTANCE_ID));
        if (v == null) {
            try {
                v = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw Throwables.propagate(e);
            }
        }
        return v;
    }

    /**
     * Get the App name.
     *
     * @return The App name or null.
     */
    public static String app() {
        return getenv(APP, UNKNOWN);
    }

    /**
     * Get the cluster.
     *
     * @return The cluster name or null.
     */
    public static String cluster() {
        return getenv(CLUSTER, UNKNOWN);
    }

    /**
     * Get the autoScalingGroup name.
     *
     * @return The autoScalingGroup or null.
     */
    public static String asg() {
        return getenv(ASG, UNKNOWN);
    }

    /**
     * Get the current stack.
     *
     * @return The stack (main for example) or null.
     */
    public static String stack() {
        return getenv(STACK, UNKNOWN);
    }

    /**
     * Get the netflix environment.
     *
     * @return The netflix environment (prod/test) or
     * dev if we cannot determine the environment.
     */
    public static String env() {
        return getenv(ENV, "dev");
    }
}
