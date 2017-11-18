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

import com.netflix.nfgraph.compressed.BitSetOrdinalIterator;
import com.netflix.nfgraph.compressed.CompactOrdinalIterator;
import com.netflix.nfgraph.compressed.HashSetOrdinalIterator;

/**
 * <code>OrdinalIterator</code> is the interface used to iterate over a set of connections.<p>
 * 
 * An <code>OrdinalIterator</code> may be obtained for a set of connections directly from an {@link NFGraph} or via an {@link OrdinalSet}
 * obtained from an {@link NFGraph}.
 * 
 * @see CompactOrdinalIterator
 * @see HashSetOrdinalIterator
 * @see BitSetOrdinalIterator
 *
 */
public interface OrdinalIterator {
    
    /**
     * This value will be returned from <code>nextOrdinal()</code> after the iteration is completed.
     */
    public static final int NO_MORE_ORDINALS = Integer.MAX_VALUE;
    
    /**
     * @return the next ordinal in this set.
     */
    public int nextOrdinal();
    
    /**
     * Rewinds this <code>OrdinalIterator</code> to the beginning of the set.
     */
    public void reset();
    
    /**
     * Obtain a copy of this <code>OrdinalIterator</code>.  The returned <code>OrdinalIterator</code> will be reset to the beginning of the set.
     */
    public OrdinalIterator copy();
    
    /**
     * @return <code>true</code> if the ordinals returned from this set are guaranteed to be in ascending order.  Returns <code>false</code> otherwise.  
     */
    public boolean isOrdered();
    
    /**
     * An iterator which always return <code>OrdinalIterator.NO_MORE_ORDINALS</code>
     */
    public static final OrdinalIterator EMPTY_ITERATOR = new OrdinalIterator() {
        @Override public int nextOrdinal() { return NO_MORE_ORDINALS; }

        @Override public void reset() { }

        @Override public OrdinalIterator copy() { return this; }
        
        @Override public boolean isOrdered() { return true; }
    };

}
