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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Atlas stack language interpreter.
 */
public class Interpreter {

    private final Vocabulary vocabulary;

    /**
     * Create an interpreter that will use a given vocabulary.
     */
    public Interpreter(Vocabulary vocabulary) {
        this.vocabulary = vocabulary;
    }

    /**
     * Tokenize the given expression.
     */
    public static List<Object> getTokens(String expr) {
        String[] tokens = expr.trim().split("\\s*,\\s*");
        List<Object> nonEmpty = Lists.newArrayListWithCapacity(tokens.length);
        for (String t : tokens) {
            if (!t.isEmpty()) {
                nonEmpty.add(t);
            }
        }
        return nonEmpty;
    }

    @SuppressWarnings("unchecked")
    void append(Deque<Object> stack, Object o) {
        List list = (List) stack.peek();
        list.add(o);
    }

    /**
     * Execute a program using the given context.
     */
    public void execute(Context context, String program) {
        execute(context, getTokens(program), 0);
    }

    /**
     * Execute a tokenized program in a given context.
     */
    public void execute(Context context, List<Object> program) {
        execute(context, program, 0);
    }

    void execute(Context context, List<Object> program, int depth) {
        if (program.isEmpty()) {
            return;
        }

        Deque<Object> stack = context.getStack();
        int listDepth = depth;
        for (Object token : program) {
            if (token.equals("(")) {
                listDepth++;
                if (listDepth == 1) {
                    stack.push(new ArrayList<>());
                } else {
                    append(stack, "(");
                }
            } else if (token.equals(")")) {
                listDepth--;
                if (listDepth > 0) {
                    append(stack, ")");
                } else if (listDepth < 0) {
                    throw new IllegalArgumentException("unbalanced paren for list");
                }
            } else if (token instanceof String && ((String) token).startsWith(":")) {
                String t = (String) token;
                if (listDepth == 0) {
                    vocabulary.execute(context, t.substring(1));
                } else {
                    append(stack, t);
                }
            } else {
                if (listDepth == 0) {
                    stack.push(token);
                } else {
                    append(stack, token);
                }
            }
        }
    }


}
