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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.compressed.NFCompressedGraph;

public class NFGraphTest {
    
    RandomizedGraphBuilder randomizedGraphBuilder;

    private long seed;
    
    private NFGraph graph;
    
    @Before
    public void setUp() throws IOException {
        Random rand = new Random();
        
        int numANodes = rand.nextInt(10000);
        int numBNodes = rand.nextInt(10000);
        
        seed = System.currentTimeMillis();
        
        randomizedGraphBuilder = new RandomizedGraphBuilder(numANodes, numBNodes);
        
        NFCompressedGraph compressedGraph = randomizedGraphBuilder.build(new Random(seed));
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        compressedGraph.writeTo(outputStream);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        this.graph = NFCompressedGraph.readFrom(inputStream);
    }

    @Test
    public void randomizedTest() {
        randomizedGraphBuilder.assertGraph(graph, new Random(seed));
    }



}
