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

package com.netflix.nfgraph.build;

import java.util.Arrays;

import com.netflix.nfgraph.OrdinalIterator;

/**
 * An implementation of {@link OrdinalIterator} returned for connections in an {@link NFBuildGraph}.
 */
public class NFBuildGraphOrdinalIterator implements OrdinalIterator {
    
    private final int ordinals[];
    
    private int currentPositionInList;
    private int previousOrdinal = Integer.MIN_VALUE;
    
    NFBuildGraphOrdinalIterator(int ordinals[], int size) {
    	this.ordinals = Arrays.copyOfRange(ordinals, 0, size);
    	Arrays.sort(this.ordinals);
    }
    
    private NFBuildGraphOrdinalIterator(int ordinals[]) {
    	this.ordinals = ordinals;
    }
    
    @Override
    public int nextOrdinal() {
        if(previousOrdinal == NO_MORE_ORDINALS)
            return NO_MORE_ORDINALS;
        
        int nextOrdinal = nextOrdinalInList();
        while(nextOrdinal == previousOrdinal) {
            nextOrdinal = nextOrdinalInList();
        }
        
        previousOrdinal = nextOrdinal;
        return nextOrdinal;
    }

    @Override
    public void reset() {
        this.previousOrdinal = 0;
        this.currentPositionInList = 0;
    }
    
    @Override
    public OrdinalIterator copy() {
        return new NFBuildGraphOrdinalIterator(ordinals); 
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    private int nextOrdinalInList() {
        if(currentPositionInList == ordinals.length)
            return NO_MORE_ORDINALS;
        return ordinals[currentPositionInList++];
    }
    
}
