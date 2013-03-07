/*
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.build.NFBuildGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

public class NFGraphTest {
    
    private int NUM_A_NODES;
    private int NUM_B_NODES;

    private static final NFGraphSpec spec = new NFGraphSpec(
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
    
    private long seed;
    
    private NFGraph graph;
    
    @Before
    public void setUp() throws IOException {
        Random rand = new Random();
        
        NUM_A_NODES = rand.nextInt(10000);
        NUM_B_NODES = rand.nextInt(10000);
        
        seed = System.currentTimeMillis();
        rand = new Random(seed);
        
        NFBuildGraph graph = new NFBuildGraph(spec);
        graph.addConnectionModel("model-1");
        graph.addConnectionModel("model-2");
        
        for(int i=0; i < NUM_A_NODES;i++) {
            if(rand.nextBoolean())
                graph.addConnection("node-type-a", i, "a-to-one-b-global", rand.nextInt(NUM_B_NODES));
            if(rand.nextBoolean())
                graph.addConnection("model-1", "node-type-a", i, "a-to-one-b-per-model", rand.nextInt(NUM_B_NODES));
            if(rand.nextBoolean())
                graph.addConnection("model-2", "node-type-a", i, "a-to-one-b-per-model", rand.nextInt(NUM_B_NODES));
        }
        
        for(int i=0; i < NUM_B_NODES;i++) {
            addMultipleRandomConnections(rand, graph, i, "global", "b-to-many-a-compact-global");
            addMultipleRandomConnections(rand, graph, i, "global", "b-to-many-a-hashed-global");
            addMultipleRandomConnections(rand, graph, i, "model-1", "b-to-many-a-compact-per-model");
            addMultipleRandomConnections(rand, graph, i, "model-2", "b-to-many-a-compact-per-model");
            addMultipleRandomConnections(rand, graph, i, "model-1", "b-to-many-a-hashed-per-model");
            addMultipleRandomConnections(rand, graph, i, "model-2", "b-to-many-a-hashed-per-model");
        }
        
        NFCompressedGraph compressedGraph = graph.compress();
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        compressedGraph.writeTo(outputStream);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        this.graph = NFCompressedGraph.readFrom(inputStream);
    }

    private void addMultipleRandomConnections(Random rand, NFBuildGraph graph, int fromOrdinal, String model, String propertyName) {
        if(rand.nextBoolean()) {
            HashSet<Integer> connections = buildRandomConnectionSet(rand);
            for(Integer connection : connections) {
                graph.addConnection(model, "node-type-b", fromOrdinal, propertyName, connection.intValue());
            }
        }
    }
    
    @Test
    public void randomizedTest() {
        Random rand = new Random(seed);
        
        for(int i=0;i<NUM_A_NODES;i++) {
            int conn = graph.getConnection("node-type-a", i, "a-to-one-b-global");
            int expected = rand.nextBoolean() ? rand.nextInt(NUM_B_NODES) : -1;
            assertEquals(expected, conn);
            
            conn = graph.getConnection("model-1", "node-type-a", i, "a-to-one-b-per-model");
            expected = rand.nextBoolean() ? rand.nextInt(NUM_B_NODES) : -1;
            assertEquals(expected, conn);

            conn = graph.getConnection("model-2", "node-type-a", i, "a-to-one-b-per-model");
            expected = rand.nextBoolean() ? rand.nextInt(NUM_B_NODES) : -1;
            assertEquals(expected, conn);
        }
        
        for(int i=0;i<NUM_B_NODES;i++) {
            assertMultipleConnections(rand, "global", i, "b-to-many-a-compact-global");
            assertMultipleConnections(rand, "global", i, "b-to-many-a-hashed-global");
            assertMultipleConnections(rand, "model-1", i, "b-to-many-a-compact-per-model");
            assertMultipleConnections(rand, "model-2", i, "b-to-many-a-compact-per-model");
            assertMultipleConnections(rand, "model-1", i, "b-to-many-a-hashed-per-model");
            assertMultipleConnections(rand, "model-2", i, "b-to-many-a-hashed-per-model");
        }
    }

    private void assertMultipleConnections(Random rand, String model, int fromOrdinal, String propertyName) {
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
            int connectedTo = rand.nextInt(NUM_A_NODES);
            while(connections.contains(connectedTo))
                connectedTo = rand.nextInt(NUM_A_NODES);
            connections.add(connectedTo);
        }
        return connections;
    }
    
}
