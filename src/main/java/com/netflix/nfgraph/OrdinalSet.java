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

import static com.netflix.nfgraph.OrdinalIterator.EMPTY_ITERATOR;
import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;

import com.netflix.nfgraph.build.NFBuildGraphOrdinalSet;
import com.netflix.nfgraph.compressed.BitSetOrdinalSet;
import com.netflix.nfgraph.compressed.CompactOrdinalSet;
import com.netflix.nfgraph.compressed.HashSetOrdinalSet;

/**
 * <code>OrdinalSet</code> is the interface used to represent a set of connections.<p>
 * 
 * An <code>OrdinalSet</code> is obtained directly from an {@link NFGraph}.
 * 
 * @see NFGraph
 * 
 */
public abstract class OrdinalSet {

    /**
     * Returns <code>true</code> when the specified value is contained in this set.  Depending on the implementation,
     * this operation will have one of two performance characteristics:<p>
     * 
     * <code>O(1)</code> for {@link HashSetOrdinalSet} and {@link BitSetOrdinalSet}<br>
     * <code>O(n)</code> for {@link CompactOrdinalSet} and {@link NFBuildGraphOrdinalSet}
     */
    public abstract boolean contains(int value);
	
    /**
     * Returns <code>true</code> when all specified values are contained in this set.  Depending on the implementation,
     * this operation will have one of two performance characteristics:<p>
     * 
     * <code>O(m)</code> for {@link HashSetOrdinalSet} and {@link BitSetOrdinalSet}, where <code>m</code> is the number of specified elements.<br>
     * <code>O(n)</code> for {@link CompactOrdinalSet}, where <code>n</code> is the number of elements in the set.<br>
     * <code>O(n * m)</code> for {@link NFBuildGraphOrdinalSet}.
     */
	public boolean containsAll(int... values) {
		for(int value : values) {
			if(!contains(value))
				return false;
		}
		return true;
	}
	
	/**
	 * Returns an array containing all elements in the set.
	 */
	public int[] asArray() {
	    int arr[] = new int[size()];
	    OrdinalIterator iter = iterator();
	    
	    int ordinal = iter.nextOrdinal();
	    int i = 0;

	    while(ordinal != NO_MORE_ORDINALS) {
	        arr[i++] = ordinal;
	        ordinal = iter.nextOrdinal();
	    }
	    
	    return arr;
	}
	
	/**
	 * @return an {@link OrdinalIterator} over this set.
	 */
	public abstract OrdinalIterator iterator();
	
	/**
	 * @return the number of ordinals in this set.
	 */
	public abstract int size();

	private static final int EMPTY_ORDINAL_ARRAY[] = new int[0];

	/**
	 * An empty <code>OrdinalSet</code>.
	 */
	public static final OrdinalSet EMPTY_SET = new OrdinalSet() {
		@Override public boolean contains(int value) { return false; }
		
		@Override public int[] asArray() { return EMPTY_ORDINAL_ARRAY; }

		@Override public OrdinalIterator iterator() { return EMPTY_ITERATOR; }

		@Override public int size() { return 0; }
	};
}
