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

package com.netflix.atlas.client.interpreter;

import com.netflix.servo.Metric;

/**
 * Utility class dealing with numbers and how we map them to boolean values.
 */
public final class Utils {
    /**
     * a double encoding our true value. Note that we consider > 0.0 == true.
     */
    static final double TRUE = 1.0;
    /**
     * a double encoding our false value.
     */
    static final double FALSE = 0.0;
    private static final double ALMOST_ZERO = 1e-8;

    private Utils() {
    }

    /**
     * Return whether the given value is true. Currently > 0.0 is considered true.
     */
    public static boolean isTrue(double d) {
        return !Double.isNaN(d) && (Math.abs(d) > ALMOST_ZERO);
    }

    static double toBooleanVal(boolean b) {
        return b ? TRUE : FALSE;
    }

    static boolean hasNumber(Metric metric) {
        return metric.hasNumberValue() && !Double.isNaN(metric.getNumberValue().doubleValue());
    }
}
