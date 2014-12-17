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

import com.netflix.servo.monitor.MonitorConfig;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Utility class for dealing with Queries.
 */
public final class Queries {

    private Queries() {
    }

    /**
     * Return a query that will evaluate to true iff both subqueries evaluate to true.
     */
    public static Query and(Query q1, Query q2) {
        if (q1 == FalseQuery.INSTANCE || q2 == FalseQuery.INSTANCE) {
            return FalseQuery.INSTANCE;
        } else if (q1 == TrueQuery.INSTANCE) {
            return q2;
        } else if (q2 == TrueQuery.INSTANCE) {
            return q1;
        } else if (q1 instanceof RegexQuery) {
            // we assume Regex queries are the most expensive,
            // so we attempt to avoid running them thanks to short-circuit and
            return new AndQuery(q2, q1);
        } else {
            return new AndQuery(q1, q2);
        }
    }

    /**
     * Return a query that will evaluate to false iff both subqueries evaluate to false.
     */
    public static Query or(Query q1, Query q2) {
        if (q1 == TrueQuery.INSTANCE || q2 == TrueQuery.INSTANCE) {
            return TrueQuery.INSTANCE;
        } else if (q1 == FalseQuery.INSTANCE) {
            return q2;
        } else if (q2 == FalseQuery.INSTANCE) {
            return q1;
        } else if (q1 instanceof RegexQuery) {
            // we assume Regex queries are the most expensive,
            // so we attempt to avoid running them thanks to short-circuit or
            return new OrQuery(q2, q1);
        } else {
            return new OrQuery(q1, q2);
        }
    }

    /**
     * Create an in query.
     */
    public static Query in(String key, String... values) {
        return new InQuery(key, Arrays.asList(values));
    }

    /**
     * Create an equals query.
     */
    public static Query eq(String key, String val) {
        return new EqualQuery(key, val);
    }

    /**
     * Create a regex query.
     */
    public static Query re(String key, String regex) {
        return new RegexQuery(key, regex);
    }

    /**
     * Return a query that will evaluate to the boolean negative of the given subquery.
     */
    public static Query not(Query q) {
        if (q == TrueQuery.INSTANCE) {
            return FalseQuery.INSTANCE;
        } else if (q == FalseQuery.INSTANCE) {
            return TrueQuery.INSTANCE;
        } else {
            return new NotQuery(q);
        }
    }

    /**
     * Returns whether a query matches a given monitor config. This differs
     * from query.apply(config) in that
     * we might cache the result and avoid re-running the query in the future.
     */
    public static boolean matches(Query query, MonitorConfig config) {
        return query.apply(config);
    }

    /**
     * Parse a given query.
     */
    public static Query parse(String expr) {
        return parseImpl(expr, Context.newDefaultContext());
    }

    /**
     * Parse the given query using an explicit set of commonTags.
     */
    public static Query parse(String expr, Map<String, String> commonTags) {
        Context context = new Context(Context.getDefaultInterpreter(), new ArrayDeque<>(), commonTags);
        return parseImpl(expr, context);
    }

    private static Query parseImpl(String expr, Context context) {
        List<Object> tokenized = Interpreter.getTokens(expr);
        Deque<Object> stack = context.execute(tokenized);
        if (stack.size() != 1) {
            throw new IllegalArgumentException("Expected one query on the stack, got "
                    + stack + " instead");
        }
        return (Query) stack.pop();
    }
}
