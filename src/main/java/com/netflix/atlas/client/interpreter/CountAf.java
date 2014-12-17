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

final class CountAf extends AbstractAf {
    CountAf(Query filter) {
        super("COUNT", filter);
    }

    @Override
    public double apply(List<Metric> updates) {
        int count = 0;
        for (Metric m : updates) {
            if (Utils.hasNumber(m) && matches(m.getConfig())) {
                ++count;
            }
        }
        return count > 0 ? count : Double.NaN;
    }

    @Override
    public ValueExpression addFilter(Query query) {
        return AggregateFunctions.count(withFilter(query));
    }
}
