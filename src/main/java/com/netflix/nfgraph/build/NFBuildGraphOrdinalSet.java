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
import com.netflix.nfgraph.OrdinalSet;

/**
 * And implementation of {@link OrdinalSet} returned for connections in an {@link NFBuildGraph}. 
 */
public class NFBuildGraphOrdinalSet extends OrdinalSet {

	private final int ordinals[];
	private final int size;
	
	public NFBuildGraphOrdinalSet(int ordinals[], int size) {
		this.ordinals = ordinals;
		this.size = size;
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
	public boolean contains(int value) {
		for(int i=0;i<size;i++) {
			if(ordinals[i] == value) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int[] asArray() {
		return Arrays.copyOf(ordinals, size);
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public OrdinalIterator iterator() {
		return new NFBuildGraphOrdinalIterator(ordinals, size);
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public int size() {
		return size;
	}
}
