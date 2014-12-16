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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A vocabulary for the atlas stack language to be used in alerts.
 */
public class AlertVocabulary implements Vocabulary {
    private static final Map<String, Word> ALERT_VOCAB = ImmutableMap.<String, Word>builder()
            // queries
            .put("has", new HasKeyWord())
            .put("eq", new EqualWord())
            .put("in", new InWord())
            .put("re", new RegexWord())
            .put("not", new NotWord())
            .put("and", new AndWord())
            .put("or", new OrWord())
            .put("false", new FalseWord())
            .put("true", new TrueWord())
                    // query helper
            .put("cq", new CommonQueryWord())
                    // aggregation
            .put("count", new CountWord())
            .put("sum", new SumWord())
            .put("min", new MinWord())
            .put("max", new MaxWord())
            .put("avg", new AvgWord())
                    // arithmetic
            .put("add", new AddWord())
            .put("sub", new SubWord())
            .put("mul", new MulWord())
            .put("div", new DivWord())
            .put("abs", new AbsWord())
            .put("sqrt", new SqrtWord())
                    // comparisons
            .put("ge", new GreaterEqualWord())
            .put("le", new LessEqualWord())
            .put("gt", new GreaterWord())
            .put("lt", new LessWord())
                    // stack
            .put("drop", new DropWord())
            .put("dup", new DupWord())
            .put("over", new OverWord())
            .put("clear", new ClearWord())
            .put("rot", new RotWord())
            .put("-rot", new ReverseRotWord())
            .put("swap", new SwapWord())
                    // standard
            .put("const", new ConstWord())
            .put("format", new FormatWord())
            .put("get", new GetWord())
            .put("set", new SetWord())
            .put("call", new CallWord())
            .put("each", new EachWord())
            .put("map", new MapWord())
            .put("list", new ListWord())
                    // things that require state
            .put("rolling-count", new RollingCountWord())
            .put("des", new DesWord()) // not implemented yet
                    // group by
            .put("by", new GroupByWord())
                    // legend only works for common tags for now
            .put("legend", new LegendWord())
                    // time related functions
            .put("time", new TimeWord())
            .build();

    private static ValueExpression fromObject(Object o) {
        ValueExpression result;
        if (o instanceof String) {
            double d = Double.parseDouble((String) o);
            result = ConstantExpression.from(d);
        } else if (o instanceof Query) {
            // default Af is sum
            result = AggregateFunctions.sum((Query) o);
        } else {
            result = (ValueExpression) o;
        }

        return result;
    }

    private static void unop(Context context, UnaryOp op) {
        // unary op:
        //  valueExpression
        //  listValueExpression
        Object a = context.getStack().pop();
        boolean gbA = a instanceof ListValueExpression;

        if (gbA) {
            ByUnaryOp e = ByUnaryOp.create((ListValueExpression) a, op);
            context.getStack().push(e);
        } else {
            context.getStack().push(ValueUnaryOp.create(fromObject(a), op));
        }
    }

    private static void binop(Context context, BinOp op) {
        // relational operators can operate on
        // VE <-> VE
        // GB <-> VE (VE <-> GB)
        // GB <-> GB
        Object b = context.getStack().pop();
        Object a = context.getStack().pop();

        boolean gbA = a instanceof ListValueExpression;
        boolean gbB = b instanceof ListValueExpression;

        if (gbA && gbB) {
            ByByRelOp e = ByByRelOp.create((ListValueExpression) a, (ListValueExpression) b, op);
            context.getStack().push(e);
        } else if (gbA) {
            ByValueRelOp e = ByValueRelOp.create((ListValueExpression) a, fromObject(b), op);
            context.getStack().push(e);
        } else if (gbB) {
            throw new IllegalArgumentException(
                    "<scalar value> REL_OP <group-by> needs to be rewritten"
                            + " as <group-by> REL_OP <scalar value>"
            );
        } else {
            context.getStack().push(ValueValueRelOp.create(fromObject(a), fromObject(b), op));
        }
    }

    @Override
    public void execute(Context context, String word) {
        Word w = ALERT_VOCAB.get(word);
        if (w == null) {
            String fmt = "no matching definition for word :%s with current stack %s";
            String errmsg = String.format(fmt, word, context.getStack());
            throw new IllegalArgumentException(errmsg);
        }

        w.execute(context);
    }

    private static class TimeWord implements Word {
        @Override
        public void execute(Context context) {
            String mode = (String) context.getStack().pop();
            Time time = Time.withMode(mode);
            context.getStack().push(time);
        }
    }

    private static class LegendWord implements Word {
        @Override
        public void execute(Context context) {
            String legendPattern = (String) context.getStack().pop();
            Object expression = context.getStack().pop();
            if (!(expression instanceof ListValueExpression)) {
                expression = MultipleExprList.from(fromObject(expression));
            }
            Legend legend = new Legend(context, (ListValueExpression) expression, legendPattern);
            context.getStack().push(legend);
        }
    }

    private static class CommonQueryWord implements Word {
        private static void cq(Context context, Query query, Expression expression) {
            // we need to rewrite the filters for all operators that take a query
            if (expression instanceof ValueExpression) {
                context.getStack().push(((ValueExpression) expression).addFilter(query));
            } else if (expression instanceof ListValueExpression) {
                context.getStack().push(((ListValueExpression) expression).addFilter(query));
            } else {
                throw new IllegalArgumentException(
                        "Cannot find proper function " + expression.getClass() + " : candidates "
                                + "cq(query, ValueExpression) -- cq(value, ListValueExpression)"
                );
            }
        }

        private static Query getQuery(Object o) {
            if (o instanceof Query) {
                return (Query) o;
            }

            if (o instanceof ConstantExpression) {
                double d = ((ConstantExpression) o).getValue();
                return Utils.isTrue(d) ? TrueQuery.INSTANCE : FalseQuery.INSTANCE;
            }

            throw new IllegalArgumentException("Cannot convert " + o + " to a query");
        }

        @Override
        public void execute(Context context) {
            Query q = getQuery(context.getStack().pop());
            Expression e = (Expression) context.getStack().pop();
            cq(context, q, e);
        }
    }

    private static class GroupByWord implements Word {
        @SuppressWarnings("unchecked")
        @Override
        public void execute(Context context) {
            List<Object> keys = (List<Object>) context.getStack().pop();
            ValueExpression e = fromObject(context.getStack().pop());
            context.getStack().push(new GroupBy(context, keys, e));
        }
    }

    private static class ConstWord implements Word {
        @Override
        public void execute(Context context) {
            String number = (String) context.getStack().pop();
            context.getStack().push(ConstantExpression.from(Double.parseDouble(number)));
        }
    }

    /**
     * Push a copy of a named value on the stack.
     */
    private static class GetWord implements Word {
        @Override
        public void execute(Context context) {
            String name = (String) context.getStack().pop();
            context.getStack().push(context.get(name));
        }
    }

    /**
     * Pop a value off the stack and store it with a given name.
     */
    private static class SetWord implements Word {
        @Override
        public void execute(Context context) {
            Object value = context.getStack().pop();
            String name = (String) context.getStack().pop();
            context.set(name, value);
        }
    }

    /**
     * Pop a list off the stack and execute it as a set of operations.
     */
    private static class CallWord implements Word {
        @SuppressWarnings("unchecked")
        @Override
        public void execute(Context context) {
            List<Object> list = (List<Object>) context.getStack().pop();
            context.execute(list);
        }
    }

    private static class ListWord implements Word {
        @Override
        public void execute(Context context) {
            List<Object> newList = Lists.newLinkedList();
            while (!context.getStack().isEmpty()) {
                Object o = context.getStack().removeLast();
                newList.add(o);
            }
            context.getStack().push(newList);
        }
    }

    /**
     * Apply a function to all elements of a list.
     */
    private static class EachWord implements Word {
        @SuppressWarnings("unchecked")
        @Override
        public void execute(Context context) {
            List<Object> f = (List<Object>) context.getStack().pop();
            List<Object> list = (List<Object>) context.getStack().pop();
            for (Object o : list) {
                context.getStack().push(o);
                context.execute(f);
            }
        }
    }

    private static class MapWord implements Word {
        @SuppressWarnings("unchecked")
        @Override
        public void execute(Context context) {
            List f = (List) context.getStack().pop();
            List list = (List) context.getStack().pop();
            List newList = Lists.newArrayListWithCapacity(list.size());

            for (Object o : list) {
                context.getStack().push(o);
                context.execute(f);
                newList.add(context.getStack().pop());
            }
            context.getStack().push(newList);
        }
    }

    /**
     * Calls java.lang.String.format.
     */
    private static class FormatWord implements Word {
        @Override
        public void execute(Context context) {
            List args = (List) context.getStack().pop();
            String fmt = (String) context.getStack().pop();
            context.getStack().push(String.format(fmt, args.toArray()));
        }
    }

    /**
     * Remove all items from the stack.
     */
    private static class ClearWord implements Word {
        @Override
        public void execute(Context context) {
            context.getStack().clear();
        }
    }

    /**
     * Remove a single item from the top of the stack.
     */
    private static class DropWord implements Word {
        @Override
        public void execute(Context context) {
            context.getStack().pop();
        }
    }

    /**
     * Duplicate the second item popped from the stack.
     */
    private static class OverWord implements Word {
        @Override
        public void execute(Context context) {
            Object o1 = context.getStack().pop();
            Object o2 = context.getStack().pop();
            context.getStack().push(o2);
            context.getStack().push(o1);
            context.getStack().push(o2);
        }
    }

    /**
     * Rotate the stack so that the item at the bottom is now at the top.
     */
    private static class RotWord implements Word {
        @Override
        public void execute(Context context) {
            // this implements the atlas definition of :rot, not the forth/factor semantics
            Object o = context.getStack().removeLast();
            context.getStack().push(o);
        }
    }

    /**
     * Swap the top two items on the stack.
     */
    private static class SwapWord implements Word {
        @Override
        public void execute(Context context) {
            Object o1 = context.getStack().pop();
            Object o2 = context.getStack().pop();
            context.getStack().push(o1);
            context.getStack().push(o2);
        }
    }

    /**
     * Rotate the stack so that the item at the top is now at the bottom.
     */
    private static class ReverseRotWord implements Word {
        @Override
        public void execute(Context context) {
            // this implements the atlas definition of :-rot
            Object o = context.getStack().pop();
            context.getStack().addLast(o);
        }
    }

    /**
     * Duplicate the item at the top of the stack.
     */
    private static class DupWord implements Word {
        @Override
        public void execute(Context context) {
            Object o = context.getStack().peek();
            context.getStack().push(o); // FIXME (needs clone() for mutable methods :( )
        }
    }

    /**
     * Query that always returns true.
     */
    private static class TrueWord implements Word {
        @Override
        public void execute(Context context) {
            context.getStack().push(TrueQuery.INSTANCE);
        }
    }

    /**
     * Query that always returns false.
     */
    private static class FalseWord implements Word {
        @Override
        public void execute(Context context) {
            context.getStack().push(FalseQuery.INSTANCE);
        }
    }

    private static class HasKeyWord implements Word {
        @Override
        public void execute(Context context) {
            String k = (String) context.getStack().pop();
            if (context.isCommonTag(k)) {
                context.getStack().push(ConstantExpression.fromBoolean(true));
            } else {
                context.getStack().push(new HasKeyQuery(k));
            }
        }
    }

    private static class EqualWord implements Word {
        @Override
        public void execute(Context context) {
            String v = (String) context.getStack().pop();
            String k = (String) context.getStack().pop();
            if (context.isCommonTag(k)) {
                boolean b = v.equals(context.getCommonTagValue(k));
                context.getStack().push(ConstantExpression.fromBoolean(b));
            } else {
                context.getStack().push(new EqualQuery(k, v));
            }
        }
    }

    private static class InWord implements Word {
        @Override
        public void execute(Context context) {
            @SuppressWarnings("unchecked")
            List<String> vs = (List<String>) context.getStack().pop();
            String k = (String) context.getStack().pop();
            if (context.isCommonTag(k)) {
                String actualValue = context.getCommonTagValue(k);
                context.getStack().push(ConstantExpression.fromBoolean(vs.contains(actualValue)));
            } else {
                context.getStack().push(new InQuery(k, vs));
            }
        }
    }

    private static class RegexWord implements Word {
        @Override
        public void execute(Context context) {
            String v = (String) context.getStack().pop();
            String k = (String) context.getStack().pop();
            if (context.isCommonTag(k)) {
                String actualValue = context.getCommonTagValue(k);
                Pattern pattern = Pattern.compile(v);
                boolean b = pattern.matcher(actualValue).find();
                context.getStack().push(ConstantExpression.fromBoolean(b));
            } else {
                context.getStack().push(new RegexQuery(k, v));
            }
        }
    }

    // these words are overloaded. They could refer to (a) quer(y|ies) or to a boolean operation
    private static class NotWord implements Word {
        @Override
        public void execute(Context context) {
            Object o = context.getStack().peek();
            if (o instanceof Query) {
                context.getStack().pop();
                context.getStack().push(Queries.not((Query) o));
            } else {
                unop(context, UnaryOp.NOT);
            }
        }
    }

    private static class AndWord implements Word {
        // simplify (and query constant)
        private static Query simplify(Object o1, Object o2) {
            Query q;
            ConstantExpression constant;

            if (o1 instanceof Query && o2 instanceof ConstantExpression) {
                q = (Query) o1;
                constant = (ConstantExpression) o2;
            } else if (o2 instanceof Query && o1 instanceof ConstantExpression) {
                q = (Query) o2;
                constant = (ConstantExpression) o1;
            } else {
                throw new IllegalArgumentException(
                        "Cannot find proper function: "
                                + "candidates and(query, query) -- and(value, value) -- "
                                + "and(query, constant)"
                );
            }

            double d = constant.getValue();
            if (Utils.isTrue(d)) {
                return q;
            } else {
                return FalseQuery.INSTANCE;
            }
        }

        @Override
        public void execute(Context context) {
            Object o2 = context.getStack().pop();
            Object o1 = context.getStack().pop();
            if (o1 instanceof Query && o2 instanceof Query) {
                context.getStack().push(Queries.and((Query) o1, (Query) o2));
            } else if (o1 instanceof Query || o2 instanceof Query) {
                Query q = simplify(o1, o2);
                context.getStack().push(q);
            } else {
                context.getStack().push(o1);
                context.getStack().push(o2);
                binop(context, BinOp.AND);
            }
        }
    }

    private static class OrWord implements Word {
        private static Query simplify(Object o1, Object o2) {
            Query q;
            ConstantExpression constant;

            if (o1 instanceof Query && o2 instanceof ConstantExpression) {
                q = (Query) o1;
                constant = (ConstantExpression) o2;
            } else if (o2 instanceof Query && o1 instanceof ConstantExpression) {
                q = (Query) o2;
                constant = (ConstantExpression) o1;
            } else {
                throw new IllegalArgumentException(
                        "Cannot find proper function: candidates "
                                + "or(query, query) -- or(value, value) -- or(query, constant)"
                );
            }

            double d = constant.getValue();
            if (Utils.isTrue(d)) {
                return TrueQuery.INSTANCE;
            } else {
                return q;
            }
        }

        @Override
        public void execute(Context context) {
            Object o2 = context.getStack().pop();
            Object o1 = context.getStack().pop();
            if (o1 instanceof Query && o2 instanceof Query) {
                context.getStack().push(Queries.or((Query) o1, (Query) o2));
            } else if (o1 instanceof Query || o2 instanceof Query) {
                Query q = simplify(o1, o2);
                context.getStack().push(q);
            } else {
                context.getStack().push(o1);
                context.getStack().push(o2);
                binop(context, BinOp.OR);
            }
        }
    }

    private static class RollingCountWord implements Word {
        @Override
        public void execute(Context context) {
            int period = Integer.parseInt((String) context.getStack().pop());
            Object o = context.getStack().pop();
            if (o instanceof ListValueExpression) {
                context.getStack().push(new ListRollingCount(context,
                        (ListValueExpression) o, period));
            } else {
                ValueExpression a = fromObject(o);
                context.getStack().push(new SingleValueRollingCount(context, a, period));
            }
        }
    }

    private static class DesWord implements Word {
        @Override
        public void execute(Context context) {
            double beta = Double.parseDouble((String) context.getStack().pop());
            double alpha = Double.parseDouble((String) context.getStack().pop());
            int trainingSize = Integer.parseInt((String) context.getStack().pop());
            ValueExpression a = fromObject(context.getStack().pop());
            context.getStack().push(new Des(a, trainingSize, alpha, beta));
        }
    }

    private static class AbsWord implements Word {
        @Override
        public void execute(Context context) {
            unop(context, UnaryOp.ABS);
        }
    }

    private static class GreaterWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.GT);
        }
    }

    private static class GreaterEqualWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.GE);
        }
    }

    private static class LessWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.LT);
        }
    }

    private static class LessEqualWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.LE);
        }
    }

    private static class MinWord implements Word {
        @Override
        public void execute(Context context) {
            Query filter = (Query) context.getStack().pop();
            context.getStack().push(AggregateFunctions.min(filter));
        }
    }

    private static class MaxWord implements Word {
        @Override
        public void execute(Context context) {
            Query filter = (Query) context.getStack().pop();
            context.getStack().push(AggregateFunctions.max(filter));
        }
    }

    private static class CountWord implements Word {
        @Override
        public void execute(Context context) {
            Query filter = (Query) context.getStack().pop();
            context.getStack().push(AggregateFunctions.count(filter));
        }
    }

    private static class SumWord implements Word {
        @Override
        public void execute(Context context) {
            Query filter = (Query) context.getStack().pop();
            context.getStack().push(AggregateFunctions.sum(filter));
        }
    }

    private static class AvgWord implements Word {
        @Override
        public void execute(Context context) {
            Query filter = (Query) context.getStack().pop();
            context.getStack().push(AggregateFunctions.avg(filter));
        }
    }

    private static class AddWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.ADD);
        }
    }

    private static class SubWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.SUB);
        }
    }

    private static class MulWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.MUL);
        }
    }

    private static class SqrtWord implements Word {
        @Override
        public void execute(Context context) {
            unop(context, UnaryOp.SQRT);
        }
    }

    private static class DivWord implements Word {
        @Override
        public void execute(Context context) {
            binop(context, BinOp.DIV);
        }
    }
}
