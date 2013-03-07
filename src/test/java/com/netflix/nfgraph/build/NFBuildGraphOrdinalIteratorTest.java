package com.netflix.nfgraph.build;

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.OrdinalIterator;

public class NFBuildGraphOrdinalIteratorTest {

    private NFBuildGraphOrdinalIterator iter;
    
    @Before
    public void setUp() {
        iter = new NFBuildGraphOrdinalIterator(new int[] { 2, 1, 2, 3, 4 }, 4);        
    }
    
    @Test
    public void iteratesOverOrdinalsInOrder() {
        assertEquals(1, iter.nextOrdinal());
        assertEquals(2, iter.nextOrdinal());
        assertEquals(3, iter.nextOrdinal());
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
    }
    
    @Test
    public void canBeReset() {
        for(int i=0;i<10;i++)
            iter.nextOrdinal();
        
        iter.reset();
        
        assertEquals(1, iter.nextOrdinal());
        assertEquals(2, iter.nextOrdinal());
        assertEquals(3, iter.nextOrdinal());
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
    }
    
    @Test
    public void copyContainsSameOrdinals() {
        OrdinalIterator iter = this.iter.copy();
        
        assertEquals(1, iter.nextOrdinal());
        assertEquals(2, iter.nextOrdinal());
        assertEquals(3, iter.nextOrdinal());
        assertEquals(NO_MORE_ORDINALS, iter.nextOrdinal());
    }
    
    @Test
    public void isOrdered() {
        assertEquals(true, iter.isOrdered());
    }

}
