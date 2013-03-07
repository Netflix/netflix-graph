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
 * An implementation of {@link OrdinalIterator} returned for ordinals represented as bit sets in an {@link NFCompressedGraph}.
 * 
 * @see BitSetOrdinalSet
 */
public class BitSetOrdinalIterator implements OrdinalIterator {

    private final ByteArrayReader reader;
    public int offset;
    
    public BitSetOrdinalIterator(ByteArrayReader reader) {
        this.reader = reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int nextOrdinal() {
        if(offset >>> 3 == reader.length())
            return NO_MORE_ORDINALS;
        
        skipToNextPopulatedByte();
        
        while(moreBytesToRead()) {
            if(testCurrentBit()) {
                return offset++;
            }
            offset++;
        }

        return NO_MORE_ORDINALS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        offset = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OrdinalIterator copy() {
        return new BitSetOrdinalIterator(reader);
    }
    
    /**
     * @return <code>true</code>
     */
    @Override
    public boolean isOrdered() {
        return true;
    }
    
    private void skipToNextPopulatedByte() {
        if(moreBytesToRead() 
                && (currentByte() >>> (offset & 0x07)) == 0) {
            offset += 0x08;
            offset &= ~0x07;
            
            while(moreBytesToRead() && currentByte() == 0)
                offset += 0x08;
        }
    }

    private boolean moreBytesToRead() {
        return (offset >>> 3) < reader.length();
    }

    private boolean testCurrentBit() {
        int b = currentByte();
        return (b & (1 << (offset & 0x07))) != 0;
    }
    
    private byte currentByte() {
        return reader.getByte(offset >>> 3);
    }
    
}
