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

package com.netflix.atlas.client.util;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Query the aws metadata service to access the dynamic identity information. Sample output:
 * <p/>
 * <pre>
 * curl -s 'http://instance-data/latest/dynamic/instance-identity/document'; echo
 * {
 *   "instanceId" : "i-12345678",
 *   "billingProducts" : null,
 *   "imageId" : "ami-87654321",
 *   "architecture" : "x86_64",
 *   "pendingTime" : "2014-07-15T22:27:08Z",
 *   "instanceType" : "m2.4xlarge",
 *   "accountId" : "123456789000",
 *   "kernelId" : "aki-12345678",
 *   "ramdiskId" : null,
 *   "region" : "us-east-1",
 *   "version" : "2010-08-31",
 *   "availabilityZone" : "us-east-1e",
 *   "devpayProductCodes" : null,
 *   "privateIp" : "1.2.3.4"
 * }
 * </pre>
 */
public final class AwsMetadata {

    private static final URI METADATA_URI = URI.create(
            "http://instance-data/latest/dynamic/instance-identity/document");

    private static volatile Map<String, String> instanceInfo;

    static {
        refresh(METADATA_URI);
    }

    private AwsMetadata() {
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> getInstanceInfo(URI uri) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        return (Map<String, String>) mapper.readValue(uri.toURL(), Map.class);
    }

    static void refresh(URI uri) {
        try {
            instanceInfo = getInstanceInfo(uri);
        } catch (Exception e) {
            instanceInfo = new HashMap<>();
        }
    }

    /**
     * Returns the account id from the ec2 metadata or null if it is unknown or could not be
     * retrieved.
     */
    public static String accountId() {
        return instanceInfo.get("accountId");
    }

    /**
     * Returns the instance id from the ec2 metadata or null if it is unknown or could not be
     * retrieved.
     */
    public static String instanceId() {
        return instanceInfo.get("instanceId");
    }

    /**
     * Returns the ami from the ec2 metadata or null if it is unknown or could not be
     * retrieved.
     */
    public static String ami() {
        return instanceInfo.get("imageId");
    }

    /**
     * Returns the vmtype from the ec2 metadata or null if it is unknown or could not be
     * retrieved.
     */
    public static String vmtype() {
        return instanceInfo.get("instanceType");
    }

    /**
     * Returns the zone from the ec2 metadata or null if it is unknown or could not be
     * retrieved.
     */
    public static String zone() {
        return instanceInfo.get("availabilityZone");
    }

    /**
     * Returns the region from the ec2 metadata or null if it is unknown or could not be
     * retrieved.
     */
    public static String region() {
        return instanceInfo.get("region");
    }
}
