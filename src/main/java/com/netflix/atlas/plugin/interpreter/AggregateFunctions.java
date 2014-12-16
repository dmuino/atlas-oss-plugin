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

/**
 * Utility class to create aggregate functions based on a query.
 */
final class AggregateFunctions {
    private AggregateFunctions() {
    }

    static ValueExpression sum(Query filter) {
        if (filter != FalseQuery.INSTANCE) {
            return new SumAf(filter);
        } else {
            return ConstantExpression.NaN;
        }
    }

    static ValueExpression max(Query filter) {
        if (filter != FalseQuery.INSTANCE) {
            return new MaxAf(filter);
        } else {
            return ConstantExpression.NaN;
        }
    }


    static ValueExpression avg(Query filter) {
        if (filter != FalseQuery.INSTANCE) {
            return new AvgAf(filter);
        } else {
            return ConstantExpression.NaN;
        }
    }

    static ValueExpression count(Query filter) {
        if (filter != FalseQuery.INSTANCE) {
            return new CountAf(filter);
        } else {
            return ConstantExpression.NaN;
        }
    }


    static ValueExpression min(Query filter) {
        if (filter != FalseQuery.INSTANCE) {
            return new MinAf(filter);
        } else {
            return ConstantExpression.NaN;
        }
    }
}
