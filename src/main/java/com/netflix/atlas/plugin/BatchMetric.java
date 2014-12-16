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

package com.netflix.atlas.plugin;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.servo.Metric;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.Tag;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.List;

/**
 * A metric that can report a list of values.
 */
class BatchMetric implements JsonPayload {

    private final MonitorConfig config;
    private final long step;
    private final long start;
    private final List<Number> values;

    BatchMetric(Metric m, long step) {
        this(m.getConfig(), step, m.getTimestamp(), ImmutableList.of(m.getNumberValue()));
    }

    BatchMetric(MonitorConfig config, long step, long start, List<Number> values) {
        this.config = Preconditions.checkNotNull(config);
        this.step = step;
        this.start = start;
        this.values = ImmutableList.copyOf(values);

        Preconditions.checkArgument(values.size() > 0, "value list is empty");
        if (values.size() > 1) {
            Preconditions.checkArgument(start % step == 0, "start time must be on a step boundary");
        }
    }

    MonitorConfig getConfig() {
        return config;
    }

    long getStep() {
        return step;
    }

    long getStartTime() {
        return start;
    }

    List<Number> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BatchMetric)) {
            return false;
        }
        BatchMetric m = (BatchMetric) obj;
        return config.equals(m.getConfig())
                && step == m.getStep()
                && start == m.getStartTime()
                && values.equals(m.getValues());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(config, step, start, values);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("config", config)
                .add("step", step)
                .add("start", start)
                .add("values", values)
                .toString();
    }

    @Override
    public void toJson(JsonGenerator gen) throws IOException {
        gen.writeStartObject();

        gen.writeObjectFieldStart("tags");
        gen.writeStringField("name", config.getName());
        for (Tag tag : config.getTags()) {
            gen.writeStringField(tag.getKey(), tag.getValue());
        }
        gen.writeEndObject();

        gen.writeNumberField("step", step);
        gen.writeNumberField("start", start);
        gen.writeArrayFieldStart("values");
        for (Number n : values) {
            gen.writeNumber(n.doubleValue());
        }
        gen.writeEndArray();

        gen.writeEndObject();
        gen.flush();
    }
}
