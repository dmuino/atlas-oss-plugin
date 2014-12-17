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

/**
 * Unary operations. Unary operations take a single double value and produce a double as a result.
 */
enum UnaryOp {
    /**
     * Absolute value.
     */
    ABS("ABS(%s)", new Apply() {
        @Override
        public double apply(double a) {
            return Math.abs(a);
        }
    }),

    /**
     * Square root.
     */
    SQRT("SQRT(%s)", new Apply() {
        @Override
        public double apply(double a) {
            return Math.sqrt(a);
        }
    }),

    /**
     * Boolean not.
     */
    NOT("NOT(%s)", new Apply() {
        @Override
        public double apply(double a) {
            boolean isTrue = Utils.isTrue(a);
            return Utils.toBooleanVal(!isTrue);
        }
    });

    private final Apply op;
    private final String repr;

    private UnaryOp(String repr, Apply op) {
        this.op = op;
        this.repr = repr;
    }

    double apply(double a) {
        return op.apply(a);
    }

    String getFormat() {
        return repr;
    }

    private interface Apply {
        double apply(double a);
    }
}
