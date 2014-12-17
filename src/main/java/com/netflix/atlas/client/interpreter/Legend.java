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
import com.netflix.atlas.client.util.Variables;
import com.netflix.servo.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Set a legend.
 */
final class Legend implements ListValueExpression {
    private static final Logger LOGGER = LoggerFactory.getLogger(Legend.class);
    private final Context context;
    private final ListValueExpression listExpression;
    private final String legendPattern;

    Legend(Context context, ListValueExpression listExpression, String legendPattern) {
        this.context = Preconditions.checkNotNull(context);
        this.listExpression = Preconditions.checkNotNull(listExpression);
        this.legendPattern = Preconditions.checkNotNull(legendPattern);
    }

    private String expandPattern(List<String> keyValues) {
        Map<String, String> vars = Maps.newHashMap();
        vars.putAll(context.getCommonTags());
        List<String> keyNames = listExpression.getKeys();
        int keyIdx = 0;
        for (String keyName : keyNames) {
            vars.put(keyName, keyValues.get(keyIdx));
            ++keyIdx;
        }
        LOGGER.debug("Vars for substitution={}", vars);
        return Variables.substitute(legendPattern, vars);
    }

    @Override
    public Map<List<String>, LabeledResult> apply(List<Metric> updates) {
        Map<List<String>, LabeledResult> resultMap = Maps.newHashMap();
        Map<List<String>, LabeledResult> underlyingResults = listExpression.apply(updates);
        for (Map.Entry<List<String>, LabeledResult> entry : underlyingResults.entrySet()) {
            List<String> keys = entry.getKey();
            LabeledResult labeledResult = entry.getValue();
            String legend = expandPattern(keys);
            resultMap.put(keys, new LabeledResult(legend, labeledResult.getValue()));
        }
        return resultMap;
    }

    @Override
    public ListValueExpression addFilter(Query query) {
        return new Legend(context, listExpression.addFilter(query), legendPattern);
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

        Legend legend = (Legend) o;
        return legendPattern.equals(legend.legendPattern)
                && listExpression.equals(legend.listExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(listExpression, legendPattern);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("listExpression", listExpression).
                add("legendPattern", legendPattern).
                toString();
    }
}
