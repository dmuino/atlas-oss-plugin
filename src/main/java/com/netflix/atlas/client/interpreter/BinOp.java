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
 * Binary operations. They operate on two double values and produce a double value as result.
 */
enum BinOp {
    /**
     * Greater than.
     */
    GT(">", new Apply() {
        @Override
        public double apply(double a, double b) {
            return Utils.toBooleanVal(a > b);
        }
    }),
    /**
     * Greater or equal.
     */
    GE(">=", new Apply() {
        @Override
        public double apply(double a, double b) {
            return Utils.toBooleanVal(a >= b);
        }
    }),
    /**
     * Lower than.
     */
    LT("<", new Apply() {
        @Override
        public double apply(double a, double b) {
            return Utils.toBooleanVal(a < b);
        }
    }),
    /**
     * Lower or equal.
     */
    LE("<=", new Apply() {
        @Override
        public double apply(double a, double b) {
            return Utils.toBooleanVal(a <= b);
        }
    }),
    /**
     * Add (with special handling of NaN.
     */
    ADD("+", new Apply() {
        @Override
        public double apply(double a, double b) {
            return a + b;
        }
    }),
    /**
     * Subtract.
     */
    SUB("-", new Apply() {
        @Override
        public double apply(double a, double b) {
            return a - b;
        }
    }),
    /**
     * Multiply.
     */
    MUL("*", new Apply() {
        @Override
        public double apply(double a, double b) {
            return a * b;
        }
    }),
    /**
     * Divide.
     */
    DIV("/", new Apply() {
        @Override
        public double apply(double a, double b) {
            return a / b;
        }
    }),
    /**
     * Boolean and.
     */
    AND("AND", new Apply() {
        @Override
        public double apply(double a, double b) {
            return Utils.toBooleanVal(Utils.isTrue(a) && Utils.isTrue(b));
        }
    }),
    /**
     * Boolean or.
     */
    OR("OR", new Apply() {
        @Override
        public double apply(double a, double b) {
            return Utils.toBooleanVal(Utils.isTrue(a) || Utils.isTrue(b));
        }
    });
    private final String symbol;
    private final Apply op;
    private BinOp(String symbol, Apply op) {
        this.symbol = symbol;
        this.op = op;
    }

    double apply(double a, double b) {
        return op.apply(a, b);
    }

    String getSymbol() {
        return symbol;
    }

    private interface Apply {
        double apply(double a, double b);
    }
}
