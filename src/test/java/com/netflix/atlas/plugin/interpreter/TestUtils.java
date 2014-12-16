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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.atlas.plugin.util.NetflixTagKey;
import com.netflix.servo.Metric;
import com.netflix.servo.tag.BasicTagList;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

class TestUtils {
    static final Metric tm1 = new Metric("m1", BasicTagList.of("a", "a-val", "b", "b-val1", "c", "c-val"), 0L, 1.0);
    static final Metric tm2 = new Metric("m2", BasicTagList.of("a", "a-val", "b", "b-val1", "c", "c-val"), 0L, 2.0);
    static final Metric tm3 = new Metric("m3", BasicTagList.of("a", "a-val", "b", "b-val2", "c", "c-val"), 0L, 2.9);
    static final List<Metric> groupByMetrics = ImmutableList.of(tm1, tm2, tm3);


    static final Metric eddaM1 = new Metric("edda.slot.change", BasicTagList.of("class", "a"), 0L, 1.0);
    static final Metric eddaM2 = new Metric("edda.slot.change", BasicTagList.of("class", "b"), 0L, 2.0);
    static final Metric eddaM3 = new Metric("edda.slot.change", BasicTagList.of("class", "c"), 0L, 3.0);
    static final Metric eddaM4 = new Metric("edda.slot.change", BasicTagList.of("class", "d"), 0L, 4.0);
    static final Metric eddaM5 = new Metric("edda.slot.changeX", BasicTagList.of("class", "d"), 0L, 1.0);
    static final List<Metric> edda = ImmutableList.of(eddaM1, eddaM2, eddaM3, eddaM4, eddaM5);

    static final Metric m1 = new Metric("spsFoo", BasicTagList.EMPTY, 1L, 1.0);
    static final Metric m2 = new Metric("bar", BasicTagList.EMPTY, 1L, 4.0);
    static final Metric m3 = new Metric("spsBar", BasicTagList.EMPTY, 1L, 2.0);
    static final List<Metric> updates = ImmutableList.of(m1, m2, m3);

    static final Metric sm1 = new Metric("loadavg15", BasicTagList.of("a", "a1"), 0L, 700.0);
    static final Metric sm2 = new Metric("loadavg15", BasicTagList.of("a", "a2"), 0L, 900.0);
    static final Metric sm3 = new Metric("loadavg15", BasicTagList.of("a", "a2"), 0L, 1000.0);
    static final List<Metric> samples = ImmutableList.of(sm1, sm2, sm3);

    static final Metric s3 = new Metric("s3.time1", BasicTagList.of("class", "foo"), 0L, 5000.0);
    static final Metric s4 = new Metric("s4.time2", BasicTagList.of("class", "bar"), 0L, 3000.0);
    static final Metric s5 = new Metric("s5.time3", BasicTagList.of("class", "baz"), 0L, 5000.0);
    static final List<Metric> sinceMetrics = ImmutableList.of(s3, s4, s5);
    static final Map<String, String> COMMON_TAGS = tagsFromEnvironment();

    private TestUtils() {
    }

    static Map<String, String> tagsFromEnvironment() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put(NetflixTagKey.APP, "TEST_APP");
        builder.put(NetflixTagKey.AMI, "AMI_1");
        builder.put(NetflixTagKey.ASG, "cl_name-v1");
        builder.put(NetflixTagKey.CLUSTER, "cl_name");
        builder.put(NetflixTagKey.NODE, "i-1");
        builder.put(NetflixTagKey.REGION, "us-east-1");
        builder.put(NetflixTagKey.VM_TYPE, "m1.large");
        builder.put(NetflixTagKey.ZONE, "us-east-1c");
        return builder.build();
    }

    static Context execute(String e) {
        Interpreter interpreter = new Interpreter(new AlertVocabulary());
        Context context = new Context(interpreter, new ArrayDeque<>(), COMMON_TAGS);
        interpreter.execute(context, e);
        return context;
    }

    static ListValueExpression getListExpression(String expr) {
        Interpreter interpreter = new Interpreter(new AlertVocabulary());
        Context context = new Context(interpreter, new ArrayDeque<>(), COMMON_TAGS);
        List<Object> exprProgram = Interpreter.getTokens(expr);
        return context.getListExpression(exprProgram);
    }
}
