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

package com.netflix.atlas.plugin.interpreter;

import com.netflix.servo.Metric;

import java.util.List;

final class AvgAf extends AbstractAf {

    AvgAf(Query filter) {
        super("AVG", filter);
    }

    @Override
    public double apply(List<Metric> updates) {
        double total = 0.0;
        int count = 0;
        for (Metric m : updates) {
            if (Utils.hasNumber(m) && matches(m.getConfig())) {
                total += m.getNumberValue().doubleValue();
                ++count;
            }
        }
        return count > 0 ? total / count : Double.NaN;
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return AggregateFunctions.avg(withFilter(query));
    }
}
