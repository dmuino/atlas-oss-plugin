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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.netflix.atlas.plugin.util.NetflixTagKey;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An execution Context. This class is not thread safe.
 */
public final class Context {
    private static final Interpreter defaultInterpreter = new Interpreter(new AlertVocabulary());
    private final Interpreter interpreter;
    private final Deque<Object> stack;
    private final Map<String, Object> vars = Maps.newHashMap();
    private final Map<String, String> commonTags;
    private final Cache<String, Object> state = CacheBuilder.newBuilder()
            .expireAfterAccess(15, TimeUnit.MINUTES).build();

    /**
     * Create a context with an explicit set of commonTags.
     */
    public Context(Interpreter interpreter, Deque<Object> stack, Map<String, String> commonTags) {
        this.interpreter = Preconditions.checkNotNull(interpreter);
        this.stack = Preconditions.checkNotNull(stack);
        this.commonTags = commonTags;
    }

    /**
     * Create a context with the commonTags initialized from environment variables.
     */
    public Context(Interpreter interpreter, Deque<Object> stack) {
        this(interpreter, stack, NetflixTagKey.tagsFromEnvironment());
    }

    /**
     * Get a reference to the default interpreter.
     */
    public static Interpreter getDefaultInterpreter() {
        return defaultInterpreter;
    }

    /**
     * Return a default Context.
     */
    public static Context newDefaultContext() {
        return new Context(getDefaultInterpreter(), new ArrayDeque<>());
    }

    @VisibleForTesting
    static Deque<Object> take(int desiredSize, Deque<Object> deque) {
        Deque<Object> res = new ArrayDeque<>(deque);
        while (res.size() > desiredSize) {
            res.removeLast();
        }
        return res;
    }

    /**
     * Execute the given program, and return the generated stack.
     */
    public Deque<Object> execute(List<Object> program) {
        interpreter.execute(this, program);
        return stack;
    }

    /**
     * Evaluate a program, and return a {@link ListValueExpression}.
     */
    public ListValueExpression getListExpression(List<Object> program) {
        ListValueExpression result;
        stack.clear();
        interpreter.execute(this, program);

        if (stack.size() == 1 && stack.getFirst() instanceof ListValueExpression) {
            result = (ListValueExpression) stack.getFirst();
        } else {
            // verify that we have the correct expression(s) on the stack
            for (Object o : stack) {
                if (!(o instanceof ValueExpression) && !(o instanceof ListValueExpression)) {
                    String error = String.format("Expecting a ValueExpression or ListValueExpression "
                            + "on the stack. Got %s instead", o.getClass().getSimpleName());
                    throw new IllegalArgumentException(error);
                }
            }

            // the expressions are correct
            result = MultipleExprList.fromCollection(stack);
        }

        stack.clear();
        return result;
    }


    Object get(String name) {
        Object value = vars.get(name);
        if (value == null) {
            throw new NoSuchElementException("variable " + name + " does not exist.");
        }

        return value;
    }

    public Map<String, String> getCommonTags() {
        return commonTags;
    }

    void set(String name, Object value) {
        vars.put(name, value);
    }

    boolean isCommonTag(String key) {
        return commonTags.containsKey(key);
    }

    String getCommonTagValue(String key) {
        return commonTags.get(key);
    }

    public Deque<Object> getStack() {
        return stack;
    }

    public Object getState(String uuid, Callable<?> valueLoader) throws ExecutionException {
        return state.get(uuid, valueLoader);
    }

    @VisibleForTesting
    /** Return the current state as a map. Only for testing */
    Map<String, Object> dumpState() {
        return state.asMap();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("interpreter", interpreter)
                .add("stack", stack)
                .add("vars", vars)
                .add("commonTags", commonTags)
                .add("state", state.asMap())
                .toString();
    }
}
