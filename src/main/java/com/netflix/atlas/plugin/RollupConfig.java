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

package com.netflix.atlas.plugin;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.atlas.plugin.interpreter.FalseQuery;
import com.netflix.atlas.plugin.interpreter.Queries;
import com.netflix.atlas.plugin.interpreter.Query;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Configuration for doing Rollups.
 */
public class RollupConfig {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final List<Rule> rules;

    /**
     * Create a rollup config from a list of {@link Rule}.
     */
    public RollupConfig(List<Rule> rules) {
        this.rules = Preconditions.checkNotNull(rules);
    }

    /**
     * Create a rollup config that will whitelist the metrics matched by 'query'. All original tags will be kept.
     */
    public static RollupConfig whitelist(String query) {
        Rule rule = Rule.of(query, Collections.<String>emptyList(), false);
        return new RollupConfig(ImmutableList.of(rule));
    }

    /**
     * Create a rollup config that will preserve metrics matching 'query'. For these metrics only
     * the tags in the 'tagsToKeep' parameter will be preserved. Collisions will be handled by
     * summing the values of the colliding metrics.
     *
     * @param query A string representing an atlas query.
     *              For example: <code>"name,ribbon,:re"</code>
     * @param tagsToKeep List of tag keys to keep.
     *                   For example: <code>ImmutableList.of("client", "status")</code>
     * @return A rollup config.
     */
    public static RollupConfig sumKeepingTags(String query, List<String> tagsToKeep) {
        Rule rule = Rule.of(query, tagsToKeep, true);
        return new RollupConfig(ImmutableList.of(rule));
    }

    /**
     * Create a rollup config that will preserve metrics matching 'query'. For these metrics
     * the tags in the 'tagsToDrop' parameter will be removed. Collisions will be handled by
     * summing the values of the colliding metrics.
     *
     * @param query      A string representing an atlas query.
     *                   For example: <code>"name,ribbon,:re"</code>
     * @param tagsToDrop List of tag keys to keep.
     *                   For example: <code>ImmutableList.of("client", "status")</code>
     * @return A rollup config.
     */
    public static RollupConfig sumDroppingTags(String query, List<String> tagsToDrop) {
        Rule rule = Rule.of(query, tagsToDrop, false);
        return new RollupConfig(ImmutableList.of(rule));
    }

    /**
     * Create a rollup config from a JSON definition.
     *
     * @param json A string representing a rollup config.
     *             For example: <code>[{"query": "name,sps,:eq", "rollup": [ "device", "deviceId" ]}]</code>
     * @return A rollup config.
     */
    public static RollupConfig create(String json) {
        ImmutableList.Builder<Rule> ruleBuilder = ImmutableList.builder();
        try {
            JsonNode array = MAPPER.readTree(json);
            Preconditions.checkState(array.isArray(), "Invalid Policy: Expected an array of rules.");
            Iterator<JsonNode> rulesIterator = array.getElements();
            while (rulesIterator.hasNext()) {
                JsonNode ruleNode = rulesIterator.next();
                String queryStr = ruleNode.get("query").getTextValue();
                JsonNode aggrNode = ruleNode.get("aggr");
                RollupConfig.Aggr aggr = (aggrNode == null)
                        ? Aggr.SUM
                        : Aggr.valueOf(aggrNode.getTextValue().toUpperCase());

                List<String> tags = ImmutableList.of();
                JsonNode rollupNode = ruleNode.get("rollup");
                boolean keep;
                if (rollupNode != null) {
                    keep = false;
                } else {
                    rollupNode = ruleNode.get("keep");
                    keep = rollupNode != null;
                }

                if (rollupNode != null) {
                    tags = listFrom(rollupNode);
                }
                ruleBuilder.add(Rule.of(Queries.parse(queryStr), tags, keep, aggr));
            }
            return new RollupConfig(ruleBuilder.build());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static List<String> listFrom(JsonNode node) {
        ImmutableList.Builder<String> tagsBuilder = ImmutableList.builder();
        Iterator<JsonNode> tagsIterator = node.getElements();
        while (tagsIterator.hasNext()) {
            tagsBuilder.add(tagsIterator.next().getTextValue());
        }
        return tagsBuilder.build();
    }

    /**
     * Get the list of rules that comprise this rollup config.
     *
     * @return A list of {@link com.netflix.atlas.plugin.RollupConfig.Rule}
     */
    public List<Rule> getRules() {
        return rules;
    }

    /**
     * Get a {@link com.netflix.atlas.plugin.interpreter.Query}
     * that will match any metrics relevant to this rollup config.
     *
     * @return An atlas query.
     */
    public Query getFilter() {
        if (rules.isEmpty()) {
            return FalseQuery.INSTANCE;
        }
        if (rules.size() == 1) {
            return rules.get(0).getQuery();
        }

        Query result = rules.get(0).getQuery();

        for (int i = 1; i < rules.size(); ++i) {
            result = Queries.or(result, rules.get(i).getQuery());
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RollupConfig config = (RollupConfig) o;
        return rules.equals(config.rules);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return rules.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("rules", rules).
                toString();
    }

    /**
     * Aggregation Functions. What to do with values for metrics that
     * map to the same set of tags after rollups have been applied.
     */
    public static enum Aggr {
        /**
         * Sum values.
         */
        SUM,
        /**
         * Count values.
         */
        COUNT,
        /**
         * Get the minimum value.
         */
        MIN,
        /**
         * Get the maximum value.
         */
        MAX,
        /**
         * Average values.
         */
        AVG,
        /**
         * Drop the metric.
         */
        DROP
    }

    /**
     * A rule specifies the list of tags to drop,
     * and the aggregation function to use for all
     * metrics that match a given query.
     */
    public static class Rule {
        private final Query query;
        private final Aggr aggr;
        private final List<String> tags;
        private final boolean keep;

        Rule(Query query, List<String> tags, boolean keep, Aggr aggr) {
            this.tags = Preconditions.checkNotNull(tags);
            this.query = Preconditions.checkNotNull(query);
            this.aggr = Preconditions.checkNotNull(aggr);
            this.keep = keep;
        }

        /**
         * Create a Rule from a query and a list of tags to drop, using
         * the given aggregating function.
         */
        public static Rule of(String query, List<String> tags, boolean keep) {
            return new Rule(Queries.parse(query), tags, keep, Aggr.SUM);
        }

        /**
         * Create a Rule from a query and a list of tags to drop/keep, using
         * the given aggregating function.
         */
        public static Rule of(String query, List<String> tags, boolean keepTags, Aggr aggr) {
            return new Rule(Queries.parse(query), tags, keepTags, aggr);
        }

        /**
         * Create a Rule from a query and a list of tags to drop/keep, using
         * a SUM aggregation function.
         */
        public static Rule of(Query query, List<String> tags, boolean keepTags) {
            return new Rule(query, tags, keepTags, Aggr.SUM);
        }

        /**
         * Create a Rule from a query and a list of tags to drop/keep, using
         * the given aggregating function.
         */
        public static Rule of(Query query, List<String> tags, boolean keepTags, Aggr aggr) {
            return new Rule(query, tags, keepTags, aggr);
        }

        public Query getQuery() {
            return query;
        }

        public Aggr getAggr() {
            return aggr;
        }

        public List<String> getTags() {
            return tags;
        }

        public boolean isKeep() {
            return keep;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Rule rule = (Rule) o;
            return aggr == rule.aggr && query.equals(rule.query)
                    && tags.equals(rule.tags) && keep == rule.keep;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(query, aggr, tags, keep);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("query", query)
                    .add("aggr", aggr)
                    .add("tags", tags)
                    .add("keep", keep)
                    .toString();
        }
    }
}
