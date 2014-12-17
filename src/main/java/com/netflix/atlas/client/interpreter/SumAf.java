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

import java.util.List;

final class SumAf extends AbstractAf {
    SumAf(Query filter) {
        super("SUM", filter);
    }

    @Override
    public double apply(List<Metric> updates) {
        double res = Double.NaN;
        for (Metric m : updates) {
            if (Utils.hasNumber(m) && matches(m.getConfig())) {
                double d = m.getNumberValue().doubleValue();
                if (Double.isNaN(res)) {
                    res = d;
                } else {
                    res += d;
                }
            }
        }

        return res;
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return AggregateFunctions.sum(withFilter(query));
    }
}
