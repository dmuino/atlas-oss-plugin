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

/**
 * A result for an operation, with a label describing it.
 */
public final class LabeledResult {
    private final String label;
    private final double value;

    /**
     * Create a new result for an operation using a particular label.
     */
    public LabeledResult(String label, double value) {
        this.label = Preconditions.checkNotNull(label);
        this.value = value;
    }

    public String getLabel() {
        return label;
    }

    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LabeledResult labeledResult = (LabeledResult) o;
        return Double.compare(labeledResult.value, value) == 0 && label.equals(labeledResult.label);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(label, value);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("label", label).
                add("value", value).
                toString();
    }
}
