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

package com.netflix.atlas.client;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.atlas.client.util.ValidCharacters;
import com.netflix.servo.Metric;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.List;

class UpdateRequest implements JsonPayload {
    private final TagList tags;
    private final List<BatchMetric> metrics;

    UpdateRequest(TagList tags, Metric[] metrics, int numMetrics, long step) {
        Preconditions.checkArgument(metrics.length > 0, "metrics array is empty");
        Preconditions.checkArgument(numMetrics > 0 && numMetrics <= metrics.length,
                "numMetrics is empty or out of bounds");

        ImmutableList.Builder<BatchMetric> builder = ImmutableList.builder();
        for (int i = 0; i < numMetrics; ++i) {
            Metric m = metrics[i];
            if (m.hasNumberValue()) {
                builder.add(new BatchMetric(m, step));
            }
        }

        this.tags = tags;
        this.metrics = builder.build();
    }

    TagList getTags() {
        return tags;
    }

    List<BatchMetric> getMetrics() {
        return metrics;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UpdateRequest)) {
            return false;
        }
        UpdateRequest req = (UpdateRequest) obj;
        return tags.equals(req.getTags())
                && metrics.equals(req.getMetrics());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tags, metrics);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("tags", tags)
                .add("metrics", metrics)
                .toString();
    }

    @Override
    public void toJson(JsonGenerator gen) throws IOException {
        gen.writeStartObject();

        // common tags
        gen.writeObjectFieldStart("tags");
        for (Tag tag : tags) {
            gen.writeStringField(
                    ValidCharacters.toValidCharset(tag.getKey()),
                    ValidCharacters.toValidCharset(tag.getValue()));
        }
        gen.writeEndObject();

        gen.writeArrayFieldStart("metrics");
        for (BatchMetric m : metrics) {
            m.toJson(gen);
        }
        gen.writeEndArray();

        gen.writeEndObject();
        gen.flush();
    }
}
