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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Keep a rolling count. It provides a way to get how many times a particular
 * operation returned true within a given period.
 */
final class RollingCount {
    private final Context context;
    private final String label;
    private final int period;

    RollingCount(Context context, String label, int period) {
        this.context = Preconditions.checkNotNull(context);
        Preconditions.checkArgument(period > 0);
        this.period = period;
        this.label = String.format("ROLLING-COUNT(%s, %d)",
                Preconditions.checkNotNull(label), period);
    }

    String getLabel() {
        return label;
    }

    int getPeriod() {
        return period;
    }

    private State getState() {
        try {
            Object myState = context.getState(label, new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return new State(period);
                }
            });
            return (State) myState;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    void update(boolean result) {
        getState().update(result);
    }

    double getValue() {
        return getState().getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RollingCount that = (RollingCount) o;
        return label.equals(that.label) && context.equals(that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(context, label);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("label", label)
                .add("state", getState())
                .toString();
    }

    /**
     * State for RollingCount.
     */
    @VisibleForTesting
    static class State {
        private final boolean[] values;
        private int pos;

        State(int period) {
            values = new boolean[period];
            Arrays.fill(values, false);
            pos = 0;
        }

        void update(boolean result) {
            int idx = pos++ % values.length;
            values[idx] = result;
        }

        double getValue() {
            int count = 0;
            for (boolean b : values) {
                if (b) {
                    ++count;
                }
            }
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            State state = (State) o;
            return pos == state.pos && Arrays.equals(values, state.values);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(values, pos);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("values", values)
                    .add("pos", pos)
                    .toString();
        }
    }
}
