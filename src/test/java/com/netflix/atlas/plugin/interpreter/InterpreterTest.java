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

import com.google.common.collect.ImmutableList;
import com.netflix.servo.Metric;
import com.netflix.servo.tag.BasicTagList;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class InterpreterTest {
    private static final List<Metric> updates = TestUtils.updates;
    private static final List<Metric> edda = TestUtils.edda;
    private static final List<Metric> groupByMetrics = TestUtils.groupByMetrics;
    private static final List<Metric> sinceMetrics = TestUtils.sinceMetrics;

    double eval(ValueExpression e, List<Metric> updates) {
        return e.apply(updates);
    }

    Context execute(String e) {
        return TestUtils.execute(e);
    }

    ValueExpression execAndGetValue(String e) {
        Context context = execute(e);
        assertEquals(context.getStack().size(), 1);
        return (ValueExpression) context.getStack().pop();
    }

    Query parseQuery(String e) {
        Context context = execute(e);
        assertEquals(context.getStack().size(), 1);
        return (Query) context.getStack().pop();
    }

    @Test
    public void testSum() throws Exception {
        ValueExpression sum = execAndGetValue("name,sps,:re,:sum");
        assertEquals(sum.getLabel(), "SUM(name,sps,:re)");
        assertEquals(eval(sum, updates), 3.0);
    }

    @Test
    public void testAvg() throws Exception {
        ValueExpression avg = execAndGetValue("name,sps,:re,:avg");
        assertEquals(avg.getLabel(), "AVG(name,sps,:re)");
        assertEquals(eval(avg, updates), 1.5);
    }


    @Test
    public void testGEConstant() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,2.5,:ge");
        assertEquals(e.getLabel(), "SUM(name,sps,:re) >= 2.5");
        assertEquals(eval(e, updates), Utils.TRUE);
    }

    @Test
    public void testGE_two_functions() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:ge");
        assertEquals(e.getLabel(), "SUM(name,sps,:re) >= SUM(name,bar,:eq)");
        assertEquals(eval(e, updates), Utils.FALSE);
    }

    @Test
    public void testAdd() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:add");
        assertEquals(e.getLabel(), "SUM(name,sps,:re) + SUM(name,bar,:eq)");
        assertEquals(eval(e, updates), 7.0);
    }

    @Test
    public void testAddNaN() throws Exception {
        String expr = ":false,:sum,1,:add";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        expected.push(ConstantExpression.NAN);
        assertEquals(context.getStack(), expected);
        expected.clear();

        expr = "1,2,:add";
        context = execute(expr);
        expected.push(ConstantExpression.from(3.0));
        assertEquals(context.getStack(), expected);
        expected.clear();

        // nan + nan
        expr = ":false,:count,:false,:avg,:add";
        context = execute(expr);
        expected.push(ConstantExpression.NAN);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testSub() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:sub");
        assertEquals(e.getLabel(), "SUM(name,sps,:re) - SUM(name,bar,:eq)");
        assertEquals(eval(e, updates), -1.0);
    }

    @Test
    public void testMul() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:mul");
        assertEquals(e.getLabel(), "SUM(name,sps,:re) * SUM(name,bar,:eq)");
        assertEquals(eval(e, updates), 12.0);
    }

    @Test
    public void testDiv() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:div");
        assertEquals(e.getLabel(), "SUM(name,sps,:re) / SUM(name,bar,:eq)");
        assertEquals(eval(e, updates), .75);
    }

    @Test
    public void testCount() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:count");
        assertEquals(e.getLabel(), "COUNT(name,sps,:re)");
        assertEquals(eval(e, updates), 2.0);
    }

    @Test
    public void testMin() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:min");
        assertEquals(e.getLabel(), "MIN(name,sps,:re)");
        assertEquals(eval(e, updates), 1.0);
    }

    @Test
    public void testMax() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:max");
        assertEquals(e.getLabel(), "MAX(name,sps,:re)");
        assertEquals(eval(e, updates), 2.0);
    }

    @Test
    public void testAbs() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:sub,:abs");
        assertEquals(e.getLabel(), "ABS(SUM(name,sps,:re) - SUM(name,bar,:eq))");
        assertEquals(eval(e, updates), 1.0);
    }

    @Test
    public void testSqrt() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,:sqrt");
        assertEquals(e.getLabel(), "SQRT(SUM(name,sps,:re))");
        assertEquals(eval(e, updates), Math.sqrt(3.0), 1.0e-12);
    }

    @Test
    public void testAbsList() throws Exception {
        Context c = execute("name,sps,:re,(,name,),:by,2,:sub,:abs");
        assertEquals(c.getStack().size(), 1);
        ListValueExpression e = (ListValueExpression) c.getStack().pop();
        assertEquals(e.getKeys(), ImmutableList.of("name"));
        Map<List<String>, LabeledResult> actual = e.apply(updates);
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("bar"),
                new LabeledResult("ABS(GroupBy([bar], SUM(name,sps,:re)) - 2.0)", Double.NaN));
        expected.put(ImmutableList.of("spsBar"),
                new LabeledResult("ABS(GroupBy([spsBar], SUM(name,sps,:re)) - 2.0)", 0.0));
        expected.put(ImmutableList.of("spsFoo"),
                new LabeledResult("ABS(GroupBy([spsFoo], SUM(name,sps,:re)) - 2.0)", 1.0));
        assertEquals(actual, expected);
    }

    @Test
    public void testGt() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:sub,:abs,0.9,:gt");
        assertEquals(e.getLabel(), "ABS(SUM(name,sps,:re) - SUM(name,bar,:eq)) > 0.9");
        assertEquals(eval(e, updates), Utils.TRUE);
    }

    @Test
    public void testLt() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:sub,:abs,1.0,:lt");
        assertEquals(e.getLabel(), "ABS(SUM(name,sps,:re) - SUM(name,bar,:eq)) < 1.0");
        assertEquals(eval(e, updates), Utils.FALSE);
    }

    @Test
    public void testRollingCount() throws Exception {
        ValueExpression e = execAndGetValue("name,sps,:re,:sum,name,bar,:eq,:sum,:sub,:abs,1.0,:le,4,:rolling-count");
        assertEquals(e.getLabel(), "ROLLING-COUNT(ABS(SUM(name,sps,:re) - SUM(name,bar,:eq)) <= 1.0, 4)");
        assertEquals(eval(e, updates), 1.0);
        assertEquals(eval(e, updates), 2.0);
        assertEquals(eval(e, updates), 3.0);
        assertEquals(eval(e, updates), 4.0);
        assertEquals(eval(e, updates), 4.0);
        assertEquals(eval(e, updates), 4.0);
    }

    @Test
    public void testDes() throws Exception {

    }

    @Test
    public void testBooleanAnd() throws Exception {
        ValueExpression f = execAndGetValue("1,0,:and");
        ValueExpression t = execAndGetValue("2,1,:and");
        // due to constant folding we simplify to the following:
        assertEquals(f.getLabel(), "0.0");
        assertEquals(t.getLabel(), "1.0");
        assertEquals(eval(f, updates), Utils.FALSE);
        assertEquals(eval(t, updates), Utils.TRUE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBooleanAndQueryValue() throws Exception {
        execAndGetValue("name,sps,:re,0,:and");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBooleanAndQueryValue2() throws Exception {
        execAndGetValue("0,name,sps,:re,:and");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBooleanOrQueryValue() throws Exception {
        execAndGetValue("name,sps,:re,0,:or");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBooleanOrQueryValue2() throws Exception {
        execAndGetValue("0,name,sps,:re,:or");
    }


    @Test
    public void testBooleanOr() throws Exception {
        ValueExpression t1 = execAndGetValue("1,0,:or");
        ValueExpression t2 = execAndGetValue("2,1,:or");
        ValueExpression f = execAndGetValue("0,0,:or");
        assertEquals(t1.getLabel(), "1.0");
        assertEquals(t2.getLabel(), "1.0");
        assertEquals(f.getLabel(), "0.0");
        assertEquals(eval(t1, updates), Utils.TRUE);
        assertEquals(eval(t2, updates), Utils.TRUE);
        assertEquals(eval(f, updates), Utils.FALSE);
    }

    @Test
    public void testBooleanNot() throws Exception {
        ValueExpression f = execAndGetValue("2,:not");
        ValueExpression t = execAndGetValue("0,:not");
        assertEquals(f.getLabel(), "0.0");
        assertEquals(t.getLabel(), "1.0");
        assertEquals(eval(f, updates), Utils.FALSE);
        assertEquals(eval(t, updates), Utils.TRUE);
    }

    @Test
    public void testTrue() throws Exception {
        assertEquals(TrueQuery.INSTANCE, parseQuery(":true"));
    }

    @Test
    public void testFalse() throws Exception {
        assertEquals(FalseQuery.INSTANCE, parseQuery(":false"));
    }

    @Test
    public void testParseHasKey() throws Exception {
        Query expected = new HasKeyQuery("a");
        assertEquals("a,:has", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    @Test
    public void testParseEqual() throws Exception {
        Query expected = new EqualQuery("a", "b");
        assertEquals("a,b,:eq", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    @Test
    public void testParseIn() throws Exception {
        Query expected = new InQuery("a", ImmutableList.of("b", "c"));
        assertEquals("a,(,b,c,),:in", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    @Test
    public void testParseRegex() throws Exception {
        Query expected = new RegexQuery("a", "^foobar.*");
        assertEquals("a,^foobar.*,:re", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    @Test
    public void testParseNot() throws Exception {
        Query expected = Queries.not(new EqualQuery("a", "b"));
        assertEquals("a,b,:eq,:not", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    @Test
    public void testParseAnd() throws Exception {
        Query expected = Queries.and(new EqualQuery("a", "b"), new EqualQuery("c", "d"));
        assertEquals("a,b,:eq,c,d,:eq,:and", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    @Test
    public void testParseOr() throws Exception {
        Query expected = Queries.or(new EqualQuery("a", "b"), new EqualQuery("c", "d"));
        assertEquals("a,b,:eq,c,d,:eq,:or", expected.toString());
        assertEquals(expected, parseQuery(expected.toString()));
    }

    Deque<Object> stack(Object... objects) {
        Deque<Object> result = new ArrayDeque<>(objects.length);
        for (Object o : objects) {
            result.push(o);
        }
        return result;
    }

    // this is just to document what our internal stack() helper
    // does
    @Test
    public void testStack() throws Exception {
        Deque<Object> st = new ArrayDeque<>();
        st.push("1");
        st.push("2");
        st.push("3");
        assertEquals(st, stack("1", "2", "3"));
    }

    // test stack vocab
    @Test
    public void testDup() throws Exception {
        String expr = "1,2,:dup";
        Context context = execute(expr);
        Deque<Object> expected = stack("1", "2", "2");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testDrop() throws Exception {
        String expr = "1,2,:drop";
        Context context = execute(expr);
        Deque<Object> expected = stack("1");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testOver() throws Exception {
        String expr = "1,2,3,:over";
        Context context = execute(expr);
        Deque<Object> expected = stack("1", "2", "3", "2");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testClear() throws Exception {
        String expr = "1,2,3,:clear";
        Context context = execute(expr);
        Deque<Object> expected = stack();
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testRot() throws Exception {
        String expr = "1,2,3,4,:rot";
        Context context = execute(expr);
        Deque<Object> expected = stack("2", "3", "4", "1");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testReverseRot() throws Exception {
        String expr = "1,2,3,4,:-rot";
        Context context = execute(expr);
        Deque<Object> expected = stack("4", "1", "2", "3");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testSwap() throws Exception {
        String expr = "1,2,3,:swap";
        Context context = execute(expr);
        Deque<Object> expected = stack("1", "3", "2");
        assertEquals(context.getStack(), expected);
    }

    // standard vocab functions
    @Test
    public void testFormat() throws Exception {
        String expr = "foo-%5s-%-5s,(,1.0,1.2,,),:format";
        Context context = execute(expr);
        Deque<Object> expected = stack("foo-  1.0-1.2  ");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testGetSet() throws Exception {
        String expr = "c0,name,sps,:re,:set,c0,:get";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        expected.add(new RegexQuery("name", "sps"));
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testCall() throws Exception {
        String expr = "1,3,(,:dup,:dup,),:call";
        Context context = execute(expr);
        Deque<Object> expected = stack("1", "3", "3", "3");
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testMap() throws Exception {
        String expr = "(,1,3,),(,1,:add,),:map";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        List<Object> newList = new ArrayList<>();
        newList.add(ValueValueRelOp.create(ConstantExpression.from(1.0), ConstantExpression.from(1.0), BinOp.ADD));
        newList.add(ValueValueRelOp.create(ConstantExpression.from(3.0), ConstantExpression.from(1.0), BinOp.ADD));
        expected.push(newList);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testEach() throws Exception {
        String expr = "(,1,3,),(,1,:add,),:each";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        expected.push(ValueValueRelOp.create(ConstantExpression.from(1.0), ConstantExpression.from(1.0), BinOp.ADD));
        expected.push(ValueValueRelOp.create(ConstantExpression.from(3.0), ConstantExpression.from(1.0), BinOp.ADD));
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testList() throws Exception {
        String expr = "1,:const,2,:const,3,:const,:list";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        List<Object> newList = new ArrayList<>();
        newList.add(ConstantExpression.from(1.0));
        newList.add(ConstantExpression.from(2.0));
        newList.add(ConstantExpression.from(3.0));
        expected.push(newList);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testParseGroupByQueryWithDefaultAF() throws Exception {
        String expr = "a,:has,(,b,c,),:by";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        List<Object> keys = ImmutableList.of("b", (Object) "c");
        ValueExpression e = AggregateFunctions.sum(new HasKeyQuery("a"));
        expected.push(new GroupBy(context, keys, e));
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testParseGroupByQuery() throws Exception {
        String expr = "a,:has,:count,(,b,c,),:by";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        List<Object> keys = ImmutableList.of("b", (Object) "c");
        ValueExpression e = AggregateFunctions.count(new HasKeyQuery("a"));
        expected.push(new GroupBy(context, keys, e));
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testGroupBy() throws Exception {
        String expr = "a,:has,(,b,c,),:by";
        Context context = execute(expr);

        ListValueExpression gb = (ListValueExpression) context.getStack().pop();
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("b-val1", "c-val"), new LabeledResult("GroupBy([b-val1,c-val], SUM(a,:has))", 3.0));
        expected.put(ImmutableList.of("b-val2", "c-val"), new LabeledResult("GroupBy([b-val2,c-val], SUM(a,:has))", 2.9));
        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testCommonTagBy() throws Exception {
        String expr = "a,:has,(,name,nf.node,),:by";
        Context context = execute(expr);

        ListValueExpression gb = (ListValueExpression) context.getStack().pop();
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("m1", "i-1"), new LabeledResult("GroupBy([m1,i-1], SUM(a,:has))", 1.0));
        expected.put(ImmutableList.of("m2", "i-1"), new LabeledResult("GroupBy([m2,i-1], SUM(a,:has))", 2.0));
        expected.put(ImmutableList.of("m3", "i-1"), new LabeledResult("GroupBy([m3,i-1], SUM(a,:has))", 2.9));
        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testRelOpByValue() throws Exception {
        String expr = "a,:has,(,b,c,),:by,3.0,:ge";
        Context context = execute(expr);

        ListValueExpression gb = (ListValueExpression) context.getStack().pop();
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("b-val1", "c-val"),
                new LabeledResult("GroupBy([b-val1,c-val], SUM(a,:has)) >= 3.0", Utils.TRUE));
        expected.put(ImmutableList.of("b-val2", "c-val"),
                new LabeledResult("GroupBy([b-val2,c-val], SUM(a,:has)) >= 3.0", Utils.FALSE));
        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testRelOpByBy() throws Exception {
        String expr = "a,:has,(,b,c,),:by,b,b-val1,:eq,(,b,c,),:by,:ge";
        Context context = execute(expr);

        ListValueExpression gb = (ListValueExpression) context.getStack().pop();
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("b-val1", "c-val"),
                new LabeledResult("GroupBy([b-val1,c-val], SUM(a,:has)) >= GroupBy([b-val1,c-val], SUM(b,b-val1,:eq))",
                        Utils.TRUE));
        expected.put(ImmutableList.of("b-val2", "c-val"),
                new LabeledResult("GroupBy([b-val2,c-val], SUM(a,:has)) >= GroupBy([b-val2,c-val], SUM(b,b-val1,:eq))",
                        Utils.FALSE));

        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testBinOpByValue() throws Exception {
        String expr = "a,:has,(,b,),:by,60,:mul";
        Context context = execute(expr);

        ListValueExpression gb = (ListValueExpression) context.getStack().pop();
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("b-val1"),
                new LabeledResult("GroupBy([b-val1], SUM(a,:has)) * 60.0",
                        180.0));
        expected.put(ImmutableList.of("b-val2"),
                new LabeledResult("GroupBy([b-val2], SUM(a,:has)) * 60.0",
                        174.0));

        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testBinOpByBy() throws Exception {
        String expr = "a,:has,(,b,),:by,b,b-val1,:eq,(,b,),:by,:mul";
        Context context = execute(expr);

        ListValueExpression gb = (ListValueExpression) context.getStack().pop();
        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("b-val1"),
                new LabeledResult("GroupBy([b-val1], SUM(a,:has)) * GroupBy([b-val1], SUM(b,b-val1,:eq))",
                        3.0 * 3.0));
        expected.put(ImmutableList.of("b-val2"),
                new LabeledResult("GroupBy([b-val2], SUM(a,:has)) * GroupBy([b-val2], SUM(b,b-val1,:eq))",
                        Double.NaN));

        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testCq() throws Exception {
        String expr = "a,:has,(,b,),:by,b,b-val1,:eq,(,b,),:by,:mul,c,c-val,:eq,:cq";
        Context context = execute(expr);
        ListValueExpression gb = (ListValueExpression) context.getStack().pop();

        Map<List<String>, LabeledResult> expected = new HashMap<>();
        expected.put(ImmutableList.of("b-val1"),
                new LabeledResult("GroupBy([b-val1], SUM(a,:has,c,c-val,:eq,:and)) * GroupBy([b-val1], SUM(b,b-val1,:eq,c,c-val,:eq,:and))",
                        3.0 * 3.0));
        expected.put(ImmutableList.of("b-val2"),
                new LabeledResult("GroupBy([b-val2], SUM(a,:has,c,c-val,:eq,:and)) * GroupBy([b-val2], SUM(b,b-val1,:eq,c,c-val,:eq,:and))",
                        Double.NaN));

        assertEquals(gb.apply(groupByMetrics), expected);
    }

    @Test
    public void testCommonTagEq() throws Exception {
        String expr = "a,:has,(,b,),:by,nf.node,i-1,:eq,(,b,),:by,:mul";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        List<String> keys = ImmutableList.of("b");
        GroupBy normal = new GroupBy(context, keys, AggregateFunctions.sum(new HasKeyQuery("a")));
        GroupBy constant = new GroupBy(context, keys, ConstantExpression.fromBoolean(true));
        expected.push(ByByRelOp.create(normal, constant, BinOp.MUL));
        assertEquals(context.getStack(), expected);
    }

    // test some optimizations

    @Test
    public void testConstantFolding() throws Exception {
        Context context = execute("name,m1,:eq,nf.node,does-not-exist,:eq,:and,:sum,10.0,:gt");
        // since nf.node != does-not-exist, the :and returns :false
        // SumAF(:false, -> Double.NaN)
        // Double.NaN > 10 ? -> FALSE
        Deque<Object> expected = new ArrayDeque<>();
        expected.push(ConstantExpression.FALSE);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testAndRegexIsLast() throws Exception {
        String expr1 = "a,b,:re,a,b,:eq,:and";
        Context context = execute(expr1);
        Deque<Object> expected = new ArrayDeque<>();
        expected.push(new AndQuery(new EqualQuery("a", "b"), new RegexQuery("a", "b")));
        assertEquals(context.getStack(), expected);
        String expr2 = "a,b,:eq,a,b,:re,:and";
        context = execute(expr2);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testOrRegexIsLast() throws Exception {
        String expr1 = "a,b,:re,a,b,:eq,:or";
        Context context = execute(expr1);
        Deque<Object> expected = new ArrayDeque<>();
        expected.push(new OrQuery(new EqualQuery("a", "b"), new RegexQuery("a", "b")));
        assertEquals(context.getStack(), expected);
        String expr2 = "a,b,:eq,a,b,:re,:or";
        context = execute(expr2);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testLegend() throws Exception {
        String expr = "nf.region,us-east-1,:eq,nf.cluster,cl_name,:eq,:and,name,loadavg15,:eq,:and,:avg,(,nf.node,),:by,800,:gt,30,:rolling-count,2,:ge,$nf.node,:legend";
        Context context = execute(expr);
        Deque<Object> expected = new ArrayDeque<>();
        GroupBy groupBy = new GroupBy(context, ImmutableList.of("nf.node"), new AvgAf(Queries.eq("name", "loadavg15")));
        ListRollingCount avgRollingCount = new ListRollingCount(context,
                ByValueRelOp.create(groupBy, ConstantExpression.from(800), BinOp.GT), 30);
        Legend legend = new Legend(context, ByValueRelOp.create(
                avgRollingCount, ConstantExpression.from(2), BinOp.GE), "$nf.node");
        expected.push(legend);
        assertEquals(context.getStack(), expected);
    }

    @Test
    public void testLegendApply() throws Exception {
        List<String> node = ImmutableList.of("i-1");
        LabeledResult expectedFalse = new LabeledResult("i-1", Utils.FALSE);
        LabeledResult expectedTrue = new LabeledResult("i-1", Utils.TRUE);

        String expr = "nf.region,us-east-1,:eq,nf.cluster,cl_name,:eq,:and,name,loadavg15,:eq,:and,:avg,(,nf.node,),:by,800,:gt,30,:rolling-count,2,:ge,$nf.node,:legend";
        Context context = execute(expr);
        Legend legend = (Legend) context.getStack().peekFirst();
        Map<List<String>, LabeledResult> res = legend.apply(TestUtils.samples);
        // rolling-count >= 2, first is going to be false
        assertEquals(res.get(node), expectedFalse);

        res = legend.apply(TestUtils.samples);
        assertEquals(res.get(node), expectedTrue);

        res = legend.apply(TestUtils.samples);
        assertEquals(res.get(node), expectedTrue);
    }

    private Map<List<String>, LabeledResult> trueValues(Map<List<String>, LabeledResult> data) {
        Map<List<String>, LabeledResult> filtered = new HashMap<>();
        for (Map.Entry<List<String>, LabeledResult> entry : data.entrySet()) {
            LabeledResult value = entry.getValue();
            if (Utils.isTrue(value.getValue())) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return filtered;
    }

    @Test
    public void testEddaSlotChange() throws Exception {
        String expr = "name,currentSlot,:eq,(,nf.asg,nf.node,),:by,name,originalSlot,:eq,(,nf.asg,nf.node,),:by,:sub,:abs,$nf.asg $nf.node,:legend,nf.app,atlas_.*,:re,class,EddaLookupService,:eq,:and,:cq";
        ListValueExpression expression = TestUtils.getListExpression(expr);
        Map<List<String>, LabeledResult> actual = expression.apply(updates);
        assertEquals(actual.size(), 1);
        Map<List<String>, LabeledResult> matches = trueValues(actual);
        assertTrue(matches.isEmpty());
    }

    @Test
    public void testWeeks() {
        String expr = "weeks,:time";
        long weeks = System.currentTimeMillis() / (7 * 24 * 60 * 60 * 1000L);
        ValueExpression expression = execAndGetValue(expr);
        assertEquals((double) weeks, expression.apply(null));
    }

    @Test
    public void testDays() {
        String expr = "days,:time";
        long days = System.currentTimeMillis() / (24 * 60 * 60 * 1000L);
        ValueExpression expression = execAndGetValue(expr);
        assertEquals((double) days, expression.apply(null));
    }

    @Test
    public void testHours() {
        String expr = "hours,:time";
        long hours = System.currentTimeMillis() / (60 * 60 * 1000L);
        ValueExpression expression = execAndGetValue(expr);
        assertEquals((double) hours, expression.apply(null));
    }

    @Test
    public void testMinutes() {
        String expr = "minutes,:time";
        long minutes = System.currentTimeMillis() / (60 * 1000L);
        ValueExpression expression = execAndGetValue(expr);
        assertEquals((double) minutes, expression.apply(null));
    }

    @Test
    void testHourOfDay() {
        int hourOfDay = (new DateTime(DateTimeZone.UTC)).getHourOfDay();
        ValueExpression expression = execAndGetValue("hourOfDay,:time");
        assertEquals((double) hourOfDay, expression.apply(null));
    }

    @Test
    void testTake() {
        Deque<Object> deque = new ArrayDeque<Object>(ImmutableList.of("1", "2", "3", "4"));
        Deque<Object> res = Context.take(2, deque);
        Deque<Object> expected = new ArrayDeque<Object>(ImmutableList.of("1", "2"));
        assertEquals(res, expected);

        Deque<Object> askingForTooMany = Context.take(10, deque);
        assertEquals(askingForTooMany, deque);
    }

    @Test
    void testMultipleMatches() {
        String expr = "name,edda.slot.change,:eq,(,nf.node,),:by,1.0,:gt,$nf.node,:legend";
        ListValueExpression expression = TestUtils.getListExpression(expr);
        Map<List<String>, LabeledResult> actual = expression.apply(edda);
        assertEquals(actual.size(), 1);
        Map<List<String>, LabeledResult> matches = trueValues(actual);
        assertTrue(!matches.isEmpty());
    }

    @Test
    void testConstantCq() {
        String expr = "name,edda.slot.change,:eq,:sum,:list,(,nf.node,i,:re,:cq,),:each";
        ValueExpression sum = execAndGetValue(expr);
        assertEquals(sum.getLabel(), "SUM(name,edda.slot.change,:eq)");
        assertEquals(eval(sum, edda), 10.0);
    }

    @Test
    void testByNameGt() {
        String expr = "name,(,s3.time1,s4.time2,),:in,(,name,),:by,4000,:gt";
        ListValueExpression gtBy = TestUtils.getListExpression(expr);
        Map<List<String>, LabeledResult> actual = gtBy.apply(sinceMetrics);
        assertEquals(actual.size(), 3);
        Map<List<String>, LabeledResult> matches = trueValues(actual);
        assertEquals(matches.size(), 1);
        List<String> key = ImmutableList.of("s3.time1");
        assertEquals(matches.get(key).getValue(), 1.0);
    }

    @Test
    void testByNameGtLegend() {
        String expr = "name,(,s3.time1,s4.time2,),:in,(,name,),:by,$name,:legend\n";
        ListValueExpression gtBy = TestUtils.getListExpression(expr);
        Map<List<String>, LabeledResult> actual = gtBy.apply(sinceMetrics);
        assertEquals(actual.size(), 3);
        Map<List<String>, LabeledResult> matches = trueValues(actual);
        assertEquals(matches.size(), 2);
    }

    Map<String, Double> fromApply(Map<List<String>, LabeledResult> applied) {
        Map<String, Double> res = new HashMap<>();
        for (LabeledResult result : applied.values()) {
            res.put(result.getLabel(), result.getValue());
        }
        return res;
    }

    @Test
    void testMultipleExprList() {
        String expr = "name,s3.time1,:eq,:sum,s3,:legend,name,s4.time2,:eq,:sum,s4,:legend";
        ListValueExpression multiple = TestUtils.getListExpression(expr);
        Map<List<String>, LabeledResult> actual = multiple.apply(sinceMetrics);
        assertEquals(actual.size(), 2);
        Map<String, Double> labelsToValues = fromApply(actual);
        assertEquals(labelsToValues.get("s3"), TestUtils.s3.getNumberValue().doubleValue());
        assertEquals(labelsToValues.get("s4"), TestUtils.s4.getNumberValue().doubleValue());
    }

    @Test
    void testValueToRollingCount() {
        String exprStr = "name,DiscoveryStatus_.*(UP),:re,(,nf.asg,nf.node,),:by,1,:rolling-count,0,:gt,$nf.asg $nf.node,:legend";
        ListValueExpression expr = TestUtils.getListExpression(exprStr);
        Metric discovery = new Metric("DiscoveryStatus_FOO_UP", BasicTagList.of("a", "a-val", "b", "b-val1", "c", "c-val"), 0L, 1.0);
        List<Metric> metrics = ImmutableList.of(discovery);
        Map<List<String>, LabeledResult> actual = expr.apply(metrics);
        Map<List<String>, LabeledResult> matches = trueValues(actual);
        assertEquals(matches.size(), 1);

        actual = expr.apply(metrics);
        matches = trueValues(actual);
        assertEquals(matches.size(), 1);
    }

    @Test
    void testNestedLists() {
        String exprStr = "name,foo,:eq,:sum,:list,(,tag,(,a,b,),:in,:cq,),:each";
        ValueExpression expr = execAndGetValue(exprStr);
        Query expectedQ = Queries.and(Queries.eq("name", "foo"), Queries.in("tag", "a", "b"));
        ValueExpression expected = new SumAf(expectedQ);
        assertEquals(expr, expected);
    }

    @Test
    void testMax0() {
        String exprStr = "name,totalNumberOfPackages,:eq,:max,100000,:const,:lt,10,:rolling-count,5,:const,:gt,$(nf.node),:legend";
        ListValueExpression expr = TestUtils.getListExpression(exprStr);
        Metric discovery = new Metric("totalNumberOfPackages", BasicTagList.of("a", "a-val", "b", "b-val1", "c", "c-val"), 0L, 0.0);
        List<Metric> metrics = ImmutableList.of(discovery);
        for (int i = 0; i < 5; i++) {
            assertEquals(trueValues(expr.apply(metrics)).size(), 0);
        }
        assertEquals(trueValues(expr.apply(metrics)).size(), 1);
    }
}
