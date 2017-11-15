package com.netflix.nfgraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.util.ByteSegmentPool;

public class NFGraphMemoryPoolTest {
    
    private ByteSegmentPool memoryPool;
    
    @Before
    public void setUp() {
        memoryPool = new ByteSegmentPool(8);
    }
    
    @Test
    public void swapBackAndForth() throws IOException {
        ByteSegmentPool memoryPool = new ByteSegmentPool(8);
        
        RandomizedGraphBuilder graphBuilder = new RandomizedGraphBuilder(10000, 10000);
        
        long seed = System.currentTimeMillis();
        
        NFCompressedGraph graph1 = graphBuilder.build(new Random(seed));
        graph1 = roundTripGraph(graph1);
        graphBuilder.assertGraph(graph1, new Random(seed));
        
        NFCompressedGraph graph2 = graphBuilder.build(new Random(seed+1));
        graph2 = roundTripGraph(graph2);
        graphBuilder.assertGraph(graph2, new Random(seed+1));
        graph1.destroy();
        
        NFCompressedGraph graph3 = graphBuilder.build(new Random(seed+2));
        graph3 = roundTripGraph(graph3);
        graphBuilder.assertGraph(graph3, new Random(seed+2));
        
        try {
            /// this shouldn't work -- we have reused this memory now.
            graphBuilder.assertGraph(graph1, new Random(seed));
            Assert.fail();
        } catch(AssertionError expected) { }
    }
    
    private NFCompressedGraph roundTripGraph(NFCompressedGraph graph) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        graph.writeTo(baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        return NFCompressedGraph.readFrom(bais, memoryPool);
    }

}
