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

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.util.ByteArrayReader;

/**
 * An implementation of {@link OrdinalIterator} returned for ordinals represented as variable-byte deltas in an {@link NFCompressedGraph}.
 * 
 * @see CompactOrdinalSet
 */
public class CompactOrdinalIterator implements OrdinalIterator {

    private final ByteArrayReader arrayReader;
    private int currentOrdinal = 0;

    CompactOrdinalIterator(ByteArrayReader arrayReader) {
        this.arrayReader = arrayReader;
    }

    @Override
    public int nextOrdinal() {
        int delta = arrayReader.readVInt();
        if(delta == -1)
            return NO_MORE_ORDINALS;
        currentOrdinal += delta;
        return currentOrdinal;
    }

    @Override
    public void reset() {
        arrayReader.reset();
        currentOrdinal = 0;
    }

    @Override
    public OrdinalIterator copy() {
        return new CompactOrdinalIterator(arrayReader.copy());
    }
    
    @Override
    public boolean isOrdered() {
        return true;
    }

}