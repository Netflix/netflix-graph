package com.netflix.nfgraph.build;


import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.exception.NFGraphException;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;
import static com.netflix.nfgraph.spec.NFPropertySpec.*;

public class NFWeightedBuildGraphTest {
    public static final String NODE_TYPE = "a";
    public static final String MULTIPLE_PROPERTY = "multiple";
    public static final String SINGLE_PROPERTY = "single";
    private NFWeightedBuildGraph buildGraph;

    @Before
    public void setUp() {
        NFGraphSpec spec = new NFGraphSpec(
                new NFNodeSpec(
                        "a",
                        new NFPropertySpec("multiple", "b", GLOBAL | MULTIPLE | COMPACT),
                        new NFPropertySpec("single", "b", GLOBAL | SINGLE | COMPACT)
                ),
                new NFNodeSpec("b")
        );
        buildGraph = new NFWeightedBuildGraph(spec);

        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 0, 1, 1);
        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 0, 1);
        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 0, 1, 2);

        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 1, 2);
        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 1, 2, 2);
        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 1, 2);

        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 2, 20);

        buildGraph.addConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY, 3, 50, 2);


        buildGraph.addConnection(NODE_TYPE, 0, SINGLE_PROPERTY, 0, 1, 1);
        buildGraph.addConnection(NODE_TYPE, 0, SINGLE_PROPERTY, 0, 1);
        buildGraph.addConnection(NODE_TYPE, 0, SINGLE_PROPERTY, 0, 1, 2);

    }

    @Test
    public void returnsValidOrdinalSetForSingleConnections() {
        WeightedOrdinalSet set = buildGraph.getWeightedConnectionSet(NODE_TYPE, 0, SINGLE_PROPERTY);

        Assert.assertEquals(1, set.size());
        Assert.assertTrue(set.contains(0));
        int[][] ordinalWithWeightAndProperty = set.asArrayWithWeightAndProperty();
        int[][] ordinalWithWeight = set.asArrayWithWeight();
        int[][] ordinalWithProperty = set.asArrayWithProperty();
        Assert.assertEquals(1, ordinalWithWeightAndProperty.length);
        // match ordinal
        Assert.assertEquals(0, ordinalWithWeightAndProperty[0][0]);
        // match weight
        Assert.assertEquals(3, ordinalWithWeightAndProperty[0][1]);
        Assert.assertEquals(3, ordinalWithWeight[0][1]);
        // match property
        Assert.assertEquals(2, ordinalWithWeightAndProperty[0][2]);
        Assert.assertEquals(2, ordinalWithProperty[0][1]);
    }

    @Test
    public void returnsValidOrdinalIteratorForSingleConnections() {
        WeightedOrdinalIterator iter = buildGraph.getWeightedConnectionIterator(NODE_TYPE, 0, SINGLE_PROPERTY);
        int[] ordinalWithWeightAndProperty = iter.nextOrdinalWithWeightAndProperty();
        Assert.assertEquals(0, ordinalWithWeightAndProperty[0]);
        Assert.assertEquals(3, ordinalWithWeightAndProperty[1]);
        Assert.assertEquals(2, ordinalWithWeightAndProperty[2]);
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinalWithWeightAndProperty()[0]);
        iter.reset();

        int[] ordinalWithWeight = iter.nextOrdinalWithWeight();
        Assert.assertEquals(0, ordinalWithWeight[0]);
        Assert.assertEquals(3, ordinalWithWeight[1]);
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinalWithWeight()[0]);
        iter.reset();

        int[] ordinalWithProperty = iter.nextOrdinalWithProperty();
        Assert.assertEquals(0, ordinalWithProperty[0]);
        Assert.assertEquals(2, ordinalWithProperty[1]);
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinalWithProperty()[0]);
        iter.reset();

        ordinalWithWeightAndProperty = iter.nextOrdinalWithWeightAndProperty();
        Assert.assertEquals(0, ordinalWithWeightAndProperty[0]);
        Assert.assertEquals(3, ordinalWithWeightAndProperty[1]);
        Assert.assertEquals(2, ordinalWithWeightAndProperty[2]);
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinalWithWeightAndProperty()[0]);

        iter.reset();
        Assert.assertEquals(0, iter.nextOrdinal());
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
    }

    @Test
    public void returnsValidOrdinalSetForMultipleConnections() {
        WeightedOrdinalSet set = buildGraph.getWeightedConnectionSet(NODE_TYPE, 0, MULTIPLE_PROPERTY);
        int[][] ordinalWithWeightAndProperty = set.asArrayWithWeightAndProperty();
        int[][] ordinalWithWeight = set.asArrayWithWeight();
        int[][] ordinalWithProperty = set.asArrayWithProperty();
        Assert.assertEquals(4, ordinalWithWeightAndProperty.length);
        Assert.assertEquals(4, ordinalWithWeight.length);
        Assert.assertEquals(4, ordinalWithProperty.length);

        Assert.assertTrue(set.contains(0));
        Assert.assertTrue(set.containsAll(1, 2, 3));

        Map<Integer, List<Integer>> expected = new HashMap<>();
        expected.put(0, new ArrayList<Integer>() {{
            add(3);
            add(2);
        }});
        expected.put(1, new ArrayList<Integer>() {{
            add(6);
            add(2);
        }});
        expected.put(2, new ArrayList<Integer>() {{
            add(20);
            add(0);
        }});
        expected.put(3, new ArrayList<Integer>() {{
            add(50);
            add(2);
        }});
        for (int[] i : ordinalWithWeightAndProperty) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(0).intValue());
            Assert.assertEquals(i[2], expected.get(i[0]).get(1).intValue());
        }

        for (int[] i : ordinalWithWeight) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(0).intValue());
        }

        for (int[] i : ordinalWithProperty) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(1).intValue());
        }
    }

    @Test
    public void returnsValidOrdinalIteratorForMultipleConnections() {
        Map<Integer, List<Integer>> expected = new HashMap<>();
        expected.put(0, new ArrayList<Integer>() {{
            add(3);
            add(2);
        }});
        expected.put(1, new ArrayList<Integer>() {{
            add(6);
            add(2);
        }});
        expected.put(2, new ArrayList<Integer>() {{
            add(20);
            add(0);
        }});
        expected.put(3, new ArrayList<Integer>() {{
            add(50);
            add(2);
        }});

        WeightedOrdinalIterator iter = buildGraph.getWeightedConnectionIterator(NODE_TYPE, 0, MULTIPLE_PROPERTY);
        int[] i = iter.nextOrdinalWithWeightAndProperty();
        int count = 0;
        while (i[0] != NO_MORE_ORDINALS) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(0).intValue());
            Assert.assertEquals(i[2], expected.get(i[0]).get(1).intValue());
            count++;
            i = iter.nextOrdinalWithWeightAndProperty();
        }
        Assert.assertEquals(4, count);
        iter.reset();

        i = iter.nextOrdinalWithWeight();
        count = 0;
        while (i[0] != NO_MORE_ORDINALS) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(0).intValue());
            count++;
            i = iter.nextOrdinalWithWeight();
        }
        Assert.assertEquals(4, count);
        iter.reset();

        i = iter.nextOrdinalWithProperty();
        count = 0;
        while (i[0] != NO_MORE_ORDINALS) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(1).intValue());
            count++;
            i = iter.nextOrdinalWithProperty();
        }
        Assert.assertEquals(4, count);
        iter.reset();

        i = iter.nextOrdinalWithWeightAndProperty();
        count = 0;
        while (i[0] != NO_MORE_ORDINALS) {
            Assert.assertTrue(expected.containsKey(i[0]));
            Assert.assertEquals(i[1], expected.get(i[0]).get(0).intValue());
            Assert.assertEquals(i[2], expected.get(i[0]).get(1).intValue());
            count++;
            i = iter.nextOrdinalWithWeightAndProperty();
        }
        Assert.assertEquals(4, count);
    }

    @Test
    public void returnsFirstOrdinalForMultipleConnections() {
        int ordinal = buildGraph.getConnection(NODE_TYPE, 0, MULTIPLE_PROPERTY);
        Assert.assertEquals(4, ordinal);
    }

    @Test
    public void returnsNegativeOneForUndefinedConnections() {
        int ordinal = buildGraph.getConnection(NODE_TYPE, 1, MULTIPLE_PROPERTY);
        Assert.assertEquals(-1, ordinal);
    }

    @Test
    public void returnsEmptySetForUndefinedConnections() {
        WeightedOrdinalSet set = buildGraph.getWeightedConnectionSet(NODE_TYPE, 1, MULTIPLE_PROPERTY);
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void returnsEmptyIteratorForUndefinedConnections() {
        WeightedOrdinalIterator iter = buildGraph.getWeightedConnectionIterator(NODE_TYPE, 1, MULTIPLE_PROPERTY);
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
        iter.reset();
        int[] ordinalWithWeightAndProperty = iter.nextOrdinalWithWeightAndProperty();
        Assert.assertEquals(NO_MORE_ORDINALS, ordinalWithWeightAndProperty[0]);
        Assert.assertEquals(WeightedOrdinalIterator.NO_MORE_WEIGHTS, ordinalWithWeightAndProperty[1]);
        Assert.assertEquals(WeightedOrdinalIterator.NO_MORE_PROPERTY, ordinalWithWeightAndProperty[2]);
    }

    @Test
    public void returnsValidOrdinalIteratorForSingleConnectionsWithCopy() {
        WeightedOrdinalIterator iter = buildGraph.getWeightedConnectionIterator(NODE_TYPE, 0, SINGLE_PROPERTY);
        Assert.assertEquals(0, iter.nextOrdinal());
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
        WeightedOrdinalIterator iter1 = (WeightedOrdinalIterator) iter.copy();
        Assert.assertEquals(0, iter1.nextOrdinal());
        Assert.assertEquals(NO_MORE_ORDINALS, iter1.nextOrdinal());
    }

    @Test
    public void returnsValidOrdinalIteratorForMultipleConnectionsWithCopy() {
        WeightedOrdinalIterator iter = buildGraph.getWeightedConnectionIterator(NODE_TYPE, 0, MULTIPLE_PROPERTY);
        Assert.assertEquals(0, iter.nextOrdinal());
        Assert.assertEquals(1, iter.nextOrdinal());
        Assert.assertEquals(2, iter.nextOrdinal());
        WeightedOrdinalIterator iter1 = (WeightedOrdinalIterator) iter.copy();
        Assert.assertEquals(3, iter.nextOrdinal());
        Assert.assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());

        Assert.assertEquals(0, iter1.nextOrdinal());
        Assert.assertEquals(1, iter1.nextOrdinal());
        Assert.assertEquals(2, iter1.nextOrdinal());
        Assert.assertEquals(3, iter1.nextOrdinal());
        Assert.assertEquals(NO_MORE_ORDINALS, iter1.nextOrdinal());
    }

    @Test
    public void throwsNFGraphExceptionWhenQueryingForUndefinedNodeType() {
        try {
            buildGraph.getWeightedConnectionSet("undefined", 0, MULTIPLE_PROPERTY);
            Assert.fail("NFGraphException should have been thrown");
        } catch (NFGraphException expected) {
        }
    }

    @Test
    public void throwsNFGraphExceptionWhenQueryingForUndefinedProperty() {
        try {
            buildGraph.getWeightedConnectionIterator(NODE_TYPE, 0, "undefined");
            Assert.fail("NFGraphException should have been thrown");
        } catch (NFGraphException expected) {
        }
    }
}