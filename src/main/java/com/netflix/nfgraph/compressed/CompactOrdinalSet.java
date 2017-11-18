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

package com.netflix.nfgraph.compressed;

import java.util.Arrays;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayReader;

/**
 * An implementation of {@link OrdinalSet}, returned for connections represented as variable-byte deltas in an {@link NFCompressedGraph}.<p>
 * 
 * A variable-byte delta representation contains between one and five bytes per connection.  
 * The ordinals in the set are sorted ascending, then encoded as the difference between each ordinal and the last ordinal.<p>
 * 
 * For example, the values [ 7, 11, 13, 21 ] will be encoded as [ 7, 4, 2, 8 ].<p>
 * 
 * This is done because smaller values can be represented in fewer bytes.<p>
 *
 * Because each value can only be derived using the previous value, <code>contains()</code> is an <code>O(n)</code> operation.<p>
 * 
 * This representation for a connection set can be configured for an {@link NFPropertySpec} using {@link NFPropertySpec#COMPACT}.
 * 
 * @see <a href="http://techblog.netflix.com/2013/01/netflixgraph-metadata-library_18.html">The Netflix Tech Blog</a>
 * @see <a href="http://en.wikipedia.org/wiki/Variable-length_quantity">Variable-length quantity</a>
 * @see <a href="https://github.com/Netflix/netflix-graph/wiki/Compact-representations">Compact Representations</a>
 */
public class CompactOrdinalSet extends OrdinalSet {

    private final ByteArrayReader reader;
    private int size = Integer.MIN_VALUE;
    
    public CompactOrdinalSet(ByteArrayReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean contains(int value) {
        OrdinalIterator iter = iterator();
        
        int iterValue = iter.nextOrdinal();
        
        while(iterValue < value) {
            iterValue = iter.nextOrdinal();
        }
        
        return iterValue == value;
    }
    
    @Override
    public boolean containsAll(int... values) {
        OrdinalIterator iter = iterator();
        
        Arrays.sort(values);
        
        int valuesIndex = 0;
        int setValue = iter.nextOrdinal();
        
        while(valuesIndex < values.length) {
            if(setValue == values[valuesIndex]) {
                valuesIndex++;
            } else if(setValue < values[valuesIndex]) {
                setValue = iter.nextOrdinal();
            } else {
                break;
            }
        }
        
        return valuesIndex == values.length;
    }

    @Override
    public OrdinalIterator iterator() {
        return new CompactOrdinalIterator(reader.copy());
    }

    @Override
    public int size() {
        if(sizeIsUnknown())
            size = countVInts(reader.copy()); 
        return size;
    }

    private boolean sizeIsUnknown() {
        return size == Integer.MIN_VALUE;
    }
    
    private int countVInts(ByteArrayReader myReader) {
        int counter = 0;
        while(myReader.readVInt() >= 0)
            counter++;
        return counter;
    }

}
