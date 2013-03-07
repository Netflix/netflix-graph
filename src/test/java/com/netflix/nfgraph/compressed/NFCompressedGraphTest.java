package com.netflix.nfgraph.compressed;

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;
import static com.netflix.nfgraph.spec.NFPropertySpec.GLOBAL;
import static com.netflix.nfgraph.spec.NFPropertySpec.MULTIPLE;
import static com.netflix.nfgraph.spec.NFPropertySpec.SINGLE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.build.NFBuildGraph;
import com.netflix.nfgraph.exception.NFGraphException;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

public class NFCompressedGraphTest {

    private NFCompressedGraph compressedGraph;
    
    @Before
    public void setUp() {
        NFGraphSpec spec = new NFGraphSpec(
                new NFNodeSpec(
                        "a",
                        new NFPropertySpec("multiple", "b", GLOBAL | MULTIPLE),
                        new NFPropertySpec("single", "b", GLOBAL | SINGLE)
                ),
                new NFNodeSpec("b")
        );
        
        NFBuildGraph graph = new NFBuildGraph(spec);
        
        graph.addConnection("a", 0, "multiple", 0);
        graph.addConnection("a", 0, "multiple", 1);
        
        graph.addConnection("a", 0, "single", 0);
        
        compressedGraph = graph.compress();
    }
    
    @Test
    public void returnsValidOrdinalSetForSingleConnections() {
        OrdinalSet set = compressedGraph.getConnectionSet("a", 0, "single");
        
        assertEquals(1, set.size());
        assertEquals(true, set.contains(0));
        assertArrayEquals(new int[] { 0 }, set.asArray());
    }
    
    @Test
    public void returnsValidOrdinalIteratorForSingleConnections() {
        OrdinalIterator iter = compressedGraph.getConnectionIterator("a", 0, "single");
        
        assertEquals(0, iter.nextOrdinal());
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
        
        iter.reset();
        
        assertEquals(0, iter.nextOrdinal());
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
    }
    
    @Test
    public void returnsFirstOrdinalForMultipleConnections() {
        int ordinal = compressedGraph.getConnection("a", 0, "multiple");
        
        assertEquals(0, ordinal);
    }
    
    @Test
    public void returnsNegativeOneForUndefinedConnections() {
        int ordinal = compressedGraph.getConnection("a", 1, "multiple");
        
        assertEquals(-1, ordinal);
    }
    
    @Test
    public void returnsEmptySetForUndefinedConnections() {
        OrdinalSet set = compressedGraph.getConnectionSet("a", 1, "multiple");
        
        assertEquals(0, set.size());
    }
    
    @Test
    public void returnsEmptyIteratorForUndefinedConnections() {
        OrdinalIterator iter = compressedGraph.getConnectionIterator("a", 1, "multiple");
        
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
    }
    
    @Test
    public void throwsNFGraphExceptionWhenQueryingForUndefinedNodeType() {
        try {
            compressedGraph.getConnectionSet("undefined", 0, "multiple");
            
            Assert.fail("NFGraphException should have been thrown");
        } catch(NFGraphException expected) { }
    }

    
    @Test
    public void throwsNFGraphExceptionWhenQueryingForUndefinedProperty() {
        try {
            compressedGraph.getConnectionIterator("a", 0, "undefined");
            
            Assert.fail("NFGraphException should have been thrown");
        } catch(NFGraphException expected) { }
    }
}
