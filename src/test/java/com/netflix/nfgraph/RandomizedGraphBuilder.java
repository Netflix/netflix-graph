package com.netflix.nfgraph;

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;
import static com.netflix.nfgraph.spec.NFPropertySpec.COMPACT;
import static com.netflix.nfgraph.spec.NFPropertySpec.GLOBAL;
import static com.netflix.nfgraph.spec.NFPropertySpec.HASH;
import static com.netflix.nfgraph.spec.NFPropertySpec.MODEL_SPECIFIC;
import static com.netflix.nfgraph.spec.NFPropertySpec.MULTIPLE;
import static com.netflix.nfgraph.spec.NFPropertySpec.SINGLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Random;

import com.netflix.nfgraph.build.NFBuildGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

public class RandomizedGraphBuilder {
    
    public static final NFGraphSpec RANDOM_GRAPH_SPEC = new NFGraphSpec(
            new NFNodeSpec("node-type-a",
                    new NFPropertySpec("a-to-one-b-global", "node-type-b", SINGLE | GLOBAL),
                    new NFPropertySpec("a-to-one-b-per-model", "node-type-b", SINGLE | MODEL_SPECIFIC)
            ),
            
            new NFNodeSpec("node-type-b", 
                    new NFPropertySpec("b-to-many-a-compact-global", "node-type-a", MULTIPLE | COMPACT | GLOBAL),
                    new NFPropertySpec("b-to-many-a-hashed-global", "node-type-a", MULTIPLE | HASH | GLOBAL),
                    new NFPropertySpec("b-to-many-a-compact-per-model", "node-type-a", MULTIPLE | COMPACT | MODEL_SPECIFIC),
                    new NFPropertySpec("b-to-many-a-hashed-per-model", "node-type-a", MULTIPLE | HASH | MODEL_SPECIFIC)
            )
    );

    private final int numANodes;
    private final int numBNodes;
    
    public RandomizedGraphBuilder(int numANodes, int numBNodes) {
        this.numANodes = numANodes;
        this.numBNodes = numBNodes;
    }
    
    public NFCompressedGraph build(Random rand) {
        NFBuildGraph graph = new NFBuildGraph(RANDOM_GRAPH_SPEC);
        graph.addConnectionModel("model-1");
        graph.addConnectionModel("model-2");
        
        for(int i=0; i < numANodes;i++) {
            if(rand.nextBoolean())
                graph.addConnection("node-type-a", i, "a-to-one-b-global", rand.nextInt(numBNodes));
            if(rand.nextBoolean())
                graph.addConnection("model-1", "node-type-a", i, "a-to-one-b-per-model", rand.nextInt(numBNodes));
            if(rand.nextBoolean())
                graph.addConnection("model-2", "node-type-a", i, "a-to-one-b-per-model", rand.nextInt(numBNodes));
        }
        
        for(int i=0; i < numBNodes;i++) {
            addMultipleRandomConnections(rand, graph, i, "global", "b-to-many-a-compact-global");
            addMultipleRandomConnections(rand, graph, i, "global", "b-to-many-a-hashed-global");
            addMultipleRandomConnections(rand, graph, i, "model-1", "b-to-many-a-compact-per-model");
            addMultipleRandomConnections(rand, graph, i, "model-2", "b-to-many-a-compact-per-model");
            addMultipleRandomConnections(rand, graph, i, "model-1", "b-to-many-a-hashed-per-model");
            addMultipleRandomConnections(rand, graph, i, "model-2", "b-to-many-a-hashed-per-model");
        }
        
        return graph.compress();
    }
    
    private void addMultipleRandomConnections(Random rand, NFBuildGraph graph, int fromOrdinal, String model, String propertyName) {
        if(rand.nextBoolean()) {
            HashSet<Integer> connections = buildRandomConnectionSet(rand);
            for(Integer connection : connections) {
                graph.addConnection(model, "node-type-b", fromOrdinal, propertyName, connection.intValue());
            }
        }
    }
    
    public void assertGraph(NFGraph graph, Random rand) {
        for(int i=0;i<numANodes;i++) {
            int conn = graph.getConnection("node-type-a", i, "a-to-one-b-global");
            int expected = rand.nextBoolean() ? rand.nextInt(numBNodes) : -1;
            assertEquals(expected, conn);
            
            conn = graph.getConnection("model-1", "node-type-a", i, "a-to-one-b-per-model");
            expected = rand.nextBoolean() ? rand.nextInt(numBNodes) : -1;
            assertEquals(expected, conn);

            conn = graph.getConnection("model-2", "node-type-a", i, "a-to-one-b-per-model");
            expected = rand.nextBoolean() ? rand.nextInt(numBNodes) : -1;
            assertEquals(expected, conn);
        }
        
        for(int i=0;i<numBNodes;i++) {
            assertMultipleConnections(graph, rand, "global", i, "b-to-many-a-compact-global");
            assertMultipleConnections(graph, rand, "global", i, "b-to-many-a-hashed-global");
            assertMultipleConnections(graph, rand, "model-1", i, "b-to-many-a-compact-per-model");
            assertMultipleConnections(graph, rand, "model-2", i, "b-to-many-a-compact-per-model");
            assertMultipleConnections(graph, rand, "model-1", i, "b-to-many-a-hashed-per-model");
            assertMultipleConnections(graph, rand, "model-2", i, "b-to-many-a-hashed-per-model");
        }

    }
    
    private void assertMultipleConnections(NFGraph graph, Random rand, String model, int fromOrdinal, String propertyName) {
        OrdinalSet set = graph.getConnectionSet(model, "node-type-b", fromOrdinal, propertyName);

        if(!rand.nextBoolean()) {
            assertEquals(0, set.size());
            return;
        }
        
        HashSet<Integer> connections = buildRandomConnectionSet(rand);
        
        OrdinalIterator iter = set.iterator();
        
        int actualOrdinal = iter.nextOrdinal();
        while(actualOrdinal != NO_MORE_ORDINALS) {
            assertTrue(String.valueOf(actualOrdinal), connections.contains(actualOrdinal));
            actualOrdinal = iter.nextOrdinal();
        }
        
        assertEquals(connections.size(), set.size());
    }

    
    private HashSet<Integer> buildRandomConnectionSet(Random rand) {
        int numConnections = rand.nextInt(100);
        HashSet<Integer> connections = new HashSet<Integer>();
        for(int j=0;j<numConnections;j++) {
            int connectedTo = rand.nextInt(numANodes);
            while(connections.contains(connectedTo))
                connectedTo = rand.nextInt(numANodes);
            connections.add(connectedTo);
        }
        return connections;
    }

}
