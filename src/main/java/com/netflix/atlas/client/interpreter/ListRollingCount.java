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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.servo.Metric;

import java.util.List;
import java.util.Map;

final class ListRollingCount implements ListValueExpression {
    private final ListValueExpression listExpression;
    private final Map<List<String>, RollingCount> rollingCountMap;
    private final int period;
    private final Context context;

    ListRollingCount(Context context, ListValueExpression listExpression, int period) {
        this.context = Preconditions.checkNotNull(context);
        this.listExpression = Preconditions.checkNotNull(listExpression);
        this.rollingCountMap = Maps.newHashMap();
        Preconditions.checkArgument(period > 0);
        this.period = period;
    }

    @Override
    public Map<List<String>, LabeledResult> apply(List<Metric> updates) {
        Map<List<String>, LabeledResult> resultMap = Maps.newHashMap();
        Map<List<String>, LabeledResult> underlyingResults = listExpression.apply(updates);
        for (Map.Entry<List<String>, LabeledResult> entry : underlyingResults.entrySet()) {
            List<String> keys = entry.getKey();
            RollingCount currentCounts = rollingCountMap.get(keys);
            if (currentCounts == null) {
                currentCounts = new RollingCount(context, entry.getValue().getLabel(), period);
                rollingCountMap.put(keys, currentCounts);
            }
            currentCounts.update(Utils.isTrue(entry.getValue().getValue()));
            resultMap.put(keys, new LabeledResult(currentCounts.getLabel(), currentCounts.getValue()));
        }
        return resultMap;
    }

    @Override
    public ListValueExpression addFilter(Query query) {
        return new ListRollingCount(context, listExpression.addFilter(query), period);
    }

    @Override
    public List<String> getKeys() {
        return listExpression.getKeys();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ListRollingCount that = (ListRollingCount) o;
        return period == that.period && listExpression.equals(that.listExpression)
                && rollingCountMap.equals(that.rollingCountMap);
    }

    @Override
    public int hashCode() {
        int result = listExpression.hashCode();
        result = 31 * result + rollingCountMap.hashCode();
        result = 31 * result + period;
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("listExpression", listExpression).
                add("rollingCountMap", rollingCountMap).
                add("period", period).
                toString();
    }
}
